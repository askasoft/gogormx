package gormfs

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/askasoft/pango/sqx"
	"github.com/askasoft/pango/str"
	"github.com/askasoft/pangox/xfs"
	"gorm.io/gorm"
)

// gfs implements xfs.XFS interface
type gfs struct {
	db *gorm.DB
	tn string // table name
}

func FS(db *gorm.DB, table string) xfs.XFS {
	return &gfs{db, table}
}

func (gfs *gfs) Open(name string) (fs.File, error) {
	f, err := gfs.FindFile(name)
	if err != nil {
		return nil, err
	}

	hf := &xfs.FSFile{XFS: gfs, File: f}
	return hf, nil
}

// FindFile find a file
func (gfs *gfs) FindFile(id string) (*xfs.File, error) {
	f := &xfs.File{}
	r := gfs.db.Table(gfs.tn).Omit("data").Where("id = ?", id).Take(f)
	if errors.Is(r.Error, gorm.ErrRecordNotFound) {
		return nil, fs.ErrNotExist
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return f, nil
}

func (gfs *gfs) SaveFile(id string, filename string, filetime time.Time, data []byte, tag ...string) (*xfs.File, error) {
	name := filepath.Base(filename)
	fext := str.ToLower(filepath.Ext(filename))

	fi := &xfs.File{
		ID:   id,
		Name: name,
		Ext:  fext,
		Tag:  str.NonEmpty(tag...),
		Size: int64(len(data)),
		Time: filetime,
		Data: data,
	}

	r := gfs.db.Table(gfs.tn).Save(fi)
	return fi, r.Error
}

func (gfs *gfs) ReadFile(id string) ([]byte, error) {
	f := &xfs.File{}
	r := gfs.db.Table(gfs.tn).Where("id = ?", id).Take(f)
	if errors.Is(r.Error, gorm.ErrRecordNotFound) {
		return nil, fs.ErrNotExist
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return f.Data, nil
}

func (gfs *gfs) CopyFile(src, dst string, tag ...string) error {
	var args []any

	sql := fmt.Sprintf("INSERT INTO %s (id, name, ext, tag, time, size, data) ", gfs.tn)
	if len(tag) == 0 {
		sql += fmt.Sprintf("SELECT ?, name, ext, tag, time, size, data FROM %s WHERE id = ?", gfs.tn)
		args = append(args, dst, src)
	} else {
		sql += fmt.Sprintf("SELECT ? AS id, name, ext, ? AS tag, time, size, data FROM %s WHERE id = ?", gfs.tn)
		args = append(args, dst, str.NonEmpty(tag...), src)
	}

	r := gfs.db.Exec(sql, args...)
	if r.Error != nil {
		return r.Error
	}
	if r.RowsAffected == 0 {
		return fs.ErrNotExist
	}
	return nil
}

func (gfs *gfs) MoveFile(src, dst string, tag ...string) error {
	tx := gfs.db.Table(gfs.tn).Where("id = ?", src)
	vs := map[string]any{"id": dst}
	if len(tag) > 0 {
		vs["tag"] = str.NonEmpty(tag...)
	}

	r := tx.Updates(vs)
	if r.Error != nil {
		return r.Error
	}
	if r.RowsAffected == 0 {
		return fs.ErrNotExist
	}
	return nil
}

func (gfs *gfs) DeleteFile(id string) error {
	r := gfs.db.Table(gfs.tn).Where("id = ?", id).Delete(&xfs.File{})
	return r.Error
}

func (gfs *gfs) DeleteFiles(ids ...string) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where("id IN ?", ids).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

func (gfs *gfs) DeletePrefix(prefix string) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where("id LIKE ?", sqx.StartsLike(prefix)).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

func (gfs *gfs) DeleteTagged(tag string) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where("tag = ?", tag).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

func (gfs *gfs) DeleteBefore(before time.Time) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where("time < ?", before).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

func (gfs *gfs) DeletePrefixBefore(prefix string, before time.Time) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where("id LIKE ? AND time < ?", sqx.StartsLike(prefix), before).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

func (gfs *gfs) DeleteTaggedBefore(tag string, before time.Time) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where("tag = ? AND time < ?", tag, before).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

func (gfs *gfs) DeleteWhere(where string, args ...any) (int64, error) {
	r := gfs.db.Table(gfs.tn).Where(where, args...).Delete(&xfs.File{})
	return r.RowsAffected, r.Error
}

// DeleteAll use "DELETE FROM files" to delete all files
func (gfs *gfs) DeleteAll() (int64, error) {
	r := gfs.db.Exec("DELETE FROM " + gfs.tn)
	return r.RowsAffected, r.Error
}

// Truncate use "TRUNCATE TABLE files" to truncate files
func (gfs *gfs) Truncate() error {
	r := gfs.db.Exec("TRUNCATE TABLE " + gfs.tn)
	return r.Error
}
