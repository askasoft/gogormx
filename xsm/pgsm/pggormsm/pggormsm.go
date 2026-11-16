package pggormsm

import (
	"errors"

	"github.com/askasoft/gogormx/gormx"
	"github.com/askasoft/pango/sqx"
	"github.com/askasoft/pangox/xsm"
	"github.com/askasoft/pangox/xsm/pgsm"
	"gorm.io/gorm"
)

type gsm struct {
	db *gorm.DB
}

func SM(db *gorm.DB) xsm.SchemaManager {
	return &gsm{db}
}

func (gsm *gsm) GetSchema(s string) (*xsm.SchemaInfo, error) {
	if pgsm.IsSysSM(s) {
		return nil, nil
	}

	tx := gsm.db.Table("pg_catalog.pg_namespace")
	tx = tx.Select(
		"nspname AS name",
		"COALESCE((SELECT SUM(pg_total_relation_size(oid)) FROM pg_catalog.pg_class WHERE relnamespace = pg_namespace.oid AND relkind = 'r'), 0) AS size",
		"COALESCE(obj_description(oid, 'pg_namespace'), '') AS comment",
	)
	tx = tx.Where("nspname = ?", s)

	schema := &xsm.SchemaInfo{}
	if err := tx.Take(schema).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return schema, nil
}

func (gsm *gsm) ExistsSchema(s string) (bool, error) {
	if pgsm.IsSysSM(s) {
		return false, nil
	}

	var sn string
	err := gsm.db.Table("pg_catalog.pg_namespace").Where("nspname = ?", s).Select("nspname").Take(&sn).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (gsm *gsm) ListSchemas() ([]string, error) {
	tx := gsm.db.Table("pg_catalog.pg_namespace").Select("nspname")
	gsm.filterSysSM(tx)
	tx.Order("nspname asc")

	rows, err := tx.Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sn string

	var ss []string
	for rows.Next() {
		if err = rows.Scan(&sn); err != nil {
			return nil, err
		}
		ss = append(ss, sn)
	}
	return ss, nil
}

func (gsm *gsm) CreateSchema(name, comment string) error {
	if pgsm.IsSysSM(name) {
		return pgsm.ErrInvalidSchemaName
	}

	err := gsm.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(pgsm.SQLCreateSchema(name)).Error; err != nil {
			return err
		}
		if comment != "" {
			if err := tx.Exec(pgsm.SQLCommentSchema(name, comment)).Error; err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (gsm *gsm) CommentSchema(name string, comment string) error {
	return gsm.db.Exec(pgsm.SQLCommentSchema(name, comment)).Error
}

func (gsm *gsm) RenameSchema(old string, new string) error {
	if pgsm.IsSysSM(old) || pgsm.IsSysSM(new) {
		return pgsm.ErrInvalidSchemaName
	}

	return gsm.db.Exec(pgsm.SQLRenameSchema(old, new)).Error
}

func (gsm *gsm) DeleteSchema(name string) error {
	if pgsm.IsSysSM(name) {
		return pgsm.ErrInvalidSchemaName
	}

	return gsm.db.Exec(pgsm.SQLDeleteSchema(name)).Error
}

func (gsm *gsm) filterSysSM(tx *gorm.DB) *gorm.DB {
	tx = tx.Where("nspname <> ?", "information_schema")
	tx = tx.Where("nspname NOT ILIKE ?", sqx.StartsLike("pg_"))
	return tx
}

func (gsm *gsm) addQuery(tx *gorm.DB, sq *xsm.SchemaQuery) {
	if sq.Name != "" {
		tx.Where("nspname ILIKE ?", sqx.StringLike(sq.Name))
	}
	gsm.filterSysSM(tx)
}

func (gsm *gsm) CountSchemas(sq *xsm.SchemaQuery) (total int, err error) {
	tx := gsm.db.Table("pg_catalog.pg_namespace")

	gsm.addQuery(tx, sq)

	var cnt int64
	err = tx.Count(&cnt).Error

	total = int(cnt)
	return
}

func (gsm *gsm) FindSchemas(sq *xsm.SchemaQuery) (schemas []*xsm.SchemaInfo, err error) {
	tx := gsm.db.Table("pg_catalog.pg_namespace")
	tx = tx.Select(
		"nspname AS name",
		"COALESCE((SELECT SUM(pg_total_relation_size(oid)) FROM pg_catalog.pg_class WHERE relnamespace = pg_namespace.oid AND relkind = 'r'), 0) AS size",
		"COALESCE(obj_description(oid, 'pg_namespace'), '') AS comment",
	)
	gsm.addQuery(tx, sq)

	gormx.Orders(tx, sq.Order, "name")
	tx.Offset(sq.Start()).Limit(sq.Limit)

	err = tx.Find(&schemas).Error
	return
}
