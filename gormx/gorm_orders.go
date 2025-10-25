package gormx

import (
	"github.com/askasoft/pango/asg"
	"github.com/askasoft/pango/str"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func addOrder(tx *gorm.DB, o string) {
	desc := str.StartsWithByte(o, '-')
	if desc {
		o = o[1:]
	}

	tx.Order(clause.OrderByColumn{Column: clause.Column{Name: o}, Desc: desc})
}

func Orders(tx *gorm.DB, order string, defaults ...string) {
	orders := str.FieldsByte(order, ',')
	defods := str.FieldsByte(str.Join(defaults, ","), ',')

	for _, o := range orders {
		addOrder(tx, o)

		if len(defods) > 0 {
			o = str.TrimPrefix(o, "-")

			defods = asg.DeleteFunc(defods, func(s string) bool {
				return o == str.TrimPrefix(s, "-")
			})
		}
	}

	for _, o := range defods {
		addOrder(tx, o)
	}
}
