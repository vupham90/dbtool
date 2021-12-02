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
				Usage:   "Get table name by sharding key. Crc for string",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "string",
						Aliases: []string{"s"},
					},
					&cli.Uint64Flag{
						Name:    "int",
						Aliases: []string{"i"},
					},
					&cli.Uint64Flag{
						Name:     "dcount",
						Required: true,
					},
					&cli.Uint64Flag{
						Name:     "tcount",
						Required: true,
					},
				},
				Action: func(c *cli.Context) error {
					var key uint64
					switch {
					case c.IsSet("string"):
						key = SCrc32(c.String("string"))
					case c.IsSet("int"):
						key = c.Uint64("int")
					}
					dcount := c.Uint64("dcount")
					tcount := c.Uint64("tcount")

					tshard := key % tcount
					dshard := tshard / (tcount / dcount)

					fmt.Printf("DB: %08d. Table: %08d\n", dshard, tshard)
					return nil
				},
			},
			{
				Name:    "crcshard",
				Aliases: []string{"cshard"},
				Usage:   "Get table name by sharding key. Crc32 everything",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "string",
						Aliases: []string{"s"},
					},
					&cli.Uint64Flag{
						Name:    "int",
						Aliases: []string{"i"},
					},
					&cli.Uint64Flag{
						Name:     "dcount",
						Required: true,
					},
					&cli.Uint64Flag{
						Name:     "tcount",
						Required: true,
					},
				},
				Action: func(c *cli.Context) error {
					var key uint64
					switch {
					case c.IsSet("string"):
						key = SCrc32(c.String("string"))
					case c.IsSet("int"):
						key = ICrc32(c.Uint64("int"))
					}
					dcount := c.Uint64("dcount")
					tcount := c.Uint64("tcount")

					tshard := key % tcount
					dshard := tshard / (tcount / dcount)

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

func SCrc32(key string) uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(strings.ToLower(key))))
}

func ICrc32(key uint64) uint64 {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(key))
	return uint64(crc32.ChecksumIEEE(ret))
}
