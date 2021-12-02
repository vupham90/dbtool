package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

var (
	ShardFlags = []cli.Flag{
		&cli.StringFlag{
			Name:    "string",
			Aliases: []string{"s"},
		},
		&cli.Uint64Flag{
			Name:    "int",
			Aliases: []string{"i"},
		},
		&cli.Uint64Flag{
			Name:     "dbcount",
			Aliases:  []string{"d"},
			Required: true,
		},
		&cli.Uint64Flag{
			Name:     "tabcount",
			Aliases:  []string{"t"},
			Required: true,
		},
	}
)

func main() {
	app := &cli.App{
		Name:  "DB Tool",
		Usage: "To provide an easy way to find sharded table",
		Commands: []*cli.Command{
			{
				Name:    "mysqlconn",
				Aliases: []string{"my"},
				Usage:   "Get MySQL command to connect to MySQL by inputting application connection string",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "conn",
						Aliases: []string{"c"},
					},
				},
				Action: func(c *cli.Context) error {
					fmt.Println(c.String("conn"))
					return nil
				},
			},
			{
				Name:    "modshard",
				Aliases: []string{"mshard"},
				Usage:   "Get table name by sharding key. Crc32 for string",
				Flags:   ShardFlags,
				Action: func(c *cli.Context) error {
					var key uint64
					switch {
					case c.IsSet("string"):
						key = SCrc32(c.String("string"))
					case c.IsSet("int"):
						key = c.Uint64("int")
					}
					dbcount := c.Uint64("dbcount")
					tabcount := c.Uint64("tabcount")

					tshard, dshard := Shard(key, dbcount, tabcount)

					fmt.Printf("DB: %08d. Table: %08d\n", dshard, tshard)
					return nil
				},
			},
			{
				Name:    "crcshard",
				Aliases: []string{"cshard"},
				Usage:   "Get table name by sharding key. Crc32 everything",
				Flags:   ShardFlags,
				Action: func(c *cli.Context) error {
					var key uint64
					switch {
					case c.IsSet("string"):
						key = SCrc32(c.String("string"))
					case c.IsSet("int"):
						key = ICrc32(c.Uint64("int"))
					}
					dbcount := c.Uint64("dbcount")
					tabcount := c.Uint64("tabcount")

					tshard, dshard := Shard(key, dbcount, tabcount)

					fmt.Printf("DB: %08d. Table: %08d\n", dshard, tshard)
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func Shard(key, dbcount, tabcount uint64) (uint64, uint64) {
	tshard := key % tabcount
	dshard := tshard / (tabcount / dbcount)
	return tshard, dshard
}

func SCrc32(key string) uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(strings.ToLower(key))))
}

func ICrc32(key uint64) uint64 {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(key))
	return uint64(crc32.ChecksumIEEE(ret))
}
