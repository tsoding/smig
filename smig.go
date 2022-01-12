package smig

import (
	"io/ioutil"
	"strings"
	"path/filepath"
	"sort"
	"fmt"
	"log"
	"database/sql"
)

type Migration struct {
	FileName string
	Query string
}

func LoadMigrationsFromFolder(sqlFolderPath string) ([]Migration, error) {
	files, err := ioutil.ReadDir(sqlFolderPath)
	if err != nil {
		return []Migration{}, err
	}

	migrations := []Migration{}

	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".sql") {
			filePath := filepath.Join(sqlFolderPath, fileName)
			query, err := ioutil.ReadFile(filePath)
			if err != nil {
				return []Migration{}, err
			}
			migrations = append(migrations,
				Migration{
					FileName: fileName,
					Query: string(query),
				})
		}
	}

	sort.Slice(migrations,
		func (i, j int) bool {
			return migrations[i].FileName < migrations[j].FileName
		})

	return migrations, nil
}

func LoadMigrationsFromDB(tx *sql.Tx) ([]Migration, error) {
	migs := []Migration{}
	rows, err := tx.Query("SELECT file_name, query FROM migrations ORDER BY file_name")
	if err != nil {
		return migs, err
	}
	defer rows.Close()
	mig := Migration{}
	for rows.Next() {
		err := rows.Scan(&mig.FileName, &mig.Query)
		migs = append(migs, mig)
		if err != nil {
			return migs, err
		}
	}
	return migs, nil
}

func ComputeUnappliedMigrations(requiredMigs []Migration, appliedMigs []Migration) ([]Migration, error) {
	migs := []Migration{}

	if len(requiredMigs) < len(appliedMigs) {
		return migs, fmt.Errorf("The database has more applied migrations than required. Applied %d but required %d", len(appliedMigs), len(requiredMigs))
	}

	for i := range(appliedMigs) {
		if requiredMigs[i].Query != appliedMigs[i].Query {
			// TODO: the unexpected migration query error is a bit too wordy
			return migs, fmt.Errorf("The content of required migration file `%s` is different from what's been applied to the database from file `%s` as migration number %d. Keep in mind that even a single character discrepancy may cause this error. Please check the file `%s` and the row of the `migrations` table where file_name = '%s' .\n\nRequired query:\n%s\nApplied query:\n%s\n",
				requiredMigs[i].FileName, appliedMigs[i].FileName, i,
				requiredMigs[i].FileName, appliedMigs[i].FileName,
				requiredMigs[i].Query, appliedMigs[i].Query)
		}
	}

	return requiredMigs[len(appliedMigs):], nil
}

func ApplyMigrationPG(tx *sql.Tx, mig *Migration) error {
	log.Println("Applying migration", mig.FileName)

	_, err := tx.Exec(mig.Query)
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO migrations (file_name, query) VALUES($1, $2)", mig.FileName, mig.Query)
	if err != nil {
		return err
	}

	return nil
}

func MigratePG(tx *sql.Tx, sqlFolderPath string) error {
	_, err := tx.Exec("CREATE TABLE IF NOT EXISTS migrations (file_name varchar(255) unique not null, query text, applied_at timestamp without time zone DEFAULT LOCALTIMESTAMP NOT NULL);")
	if err != nil {
		return err
	}

	requiredMigs, err := LoadMigrationsFromFolder(sqlFolderPath)
	if err != nil {
		return err
	}

	appliedMigs, err := LoadMigrationsFromDB(tx)
	if err != nil {
		return err
	}

	unappliedMigs, err := ComputeUnappliedMigrations(requiredMigs, appliedMigs)
	if err != nil {
		return err
	}

	for _, mig := range unappliedMigs {
		err = ApplyMigrationPG(tx, &mig)
		if err != nil {
			return err
		}
	}

	return nil
}
