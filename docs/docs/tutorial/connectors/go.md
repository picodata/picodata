# picodata-go

В данном разделе приведено описание [picodata-go] — Golang-драйвера для
работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер использует библиотеку [pgxpool] и предоставляет публичный API для
удобной работы с кластерной СУБД.

[picodata-go]: https://github.com/picodata/picodata-go
[pgxpool]: https://github.com/jackc/pgx/tree/master/pgxpool

## Подключение {: #enabling }

Пример подключения драйвера использования его в коде Golang-приложения:

```go
package main

import (
	"context"
	"fmt"
	"os"

	picogo "github.com/picodata/picodata-go"
	logger "github.com/picodata/picodata-go/logger"
	strats "github.com/picodata/picodata-go/strategies"
)

func main() {
	// export PICODATA_CONNECTION_URL=postgres://username:password@host:port
	pool, err := picogo.New(context.Background(), os.Getenv("PICODATA_CONNECTION_URL"), picogo.WithBalanceStrategy(strats.NewRoundRobinStrategy()), picogo.WithLogLevel(logger.LevelError))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	/*
	 CREATE TABLE items (id INTEGER NOT NULL,name TEXT NOT NULL,stock INTEGER,PRIMARY KEY (id)) USING memtx DISTRIBUTED BY (id) OPTION (TIMEOUT = 3.0);
	 INSERT INTO items VALUES
	 (1, 'bricks', 1123),
	 (2, 'panels', 998),
	 (3, 'piles', 177);
	*/
	var (
		id    int
		name  string
		stock int
	)
	err = pool.QueryRow(context.Background(), "select * from items where id=$1", 2).Scan(&id, &name, &stock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(id, name, stock)
}
```

## Поддерживаемые возможности {: #features }

Golang-драйвер для Picodata поддерживает следующие возможности:

- _автоматическое обнаружение инстансов кластера_ — достаточно указать
  адрес одного узла кластера, и драйвер сам наполнит пул соединений на
  основе всей топологии.
- _открытый API, с помощью которого можно балансировать нагрузку на
  кластер_ — несколько стандартных стратегий балансировки уже доступны
  "из коробки", также реализован API для реализации и использования
  своих собственных алгоритмов.
- _автоматическое управление топологией кластера_ — драйвер самостоятельно
  следит за состоянием кластера и его топологией.


## Проверка работы {: #testing }

Мы предоставляем [тестовое Go-приложение][test_app], которое позволяет проверить
функциональность Golang-драйвер при работе с Picodata. Тестовое
приложение:

- поднимает тестовый кластер посредством docker-compose
- подключается к кластеру с помощью Golang-драйвера
- позволяет выполнять [CRUD]-операции для тестовой таблицы в Picodata

[test_app]: https://github.com/picodata/picodata-go/tree/main/examples/crud
[CRUD]: https://en.wikipedia.org/wiki/CREATE,_read,_update_and_delete

Для проверки работы тестового приложения потребуются [компилятор языка
Go](https://go.dev/dl/) версии 1.24 или новее, и Docker.

Порядок действий:

1. Склонируйте репозиторий библиотеки и перейдите в директорию с тестовым приложением:
```shell
git clone https://github.com/picodata/picodata-go && cd picodata-go/examples/crud
```

2. Запустите контейнеры с тестовым кластером Picodata:
```shell
docker-compose up -d
```

3. Запустите приложение:
```shell
go run main.go
```

??? note title "Примечание"
	Попробуйте изменить LogLevel в коде тестового приложения и посмотрите как изменится вывод:

	```go
	picogo.WithLogLevel(logger.LevelError)
	```
	заменить на
	```go
	picogo.WithLogLevel(logger.LevelDebug)
	```

Результатом успешной работы будет вставка строки в тестовую таблицу и
вывод содержимого таблицы. Введите слово `help` и нажмите ++enter++. На
экране будет показан список всех доступных команд и пример использования.

```shell
(db) > help
...
(db) > list
(db) > add 'PicoData'
(db) > list
1581935770 -> 'PicoData'
(db) > update 1581935770 'Learn Go'
(db) > list
1581935770 -> 'Learn Go'
```

## Структура приложения {: #app_tree }

Ниже показано дерево файлов минимального тестового приложения:

```
├── docker-compose.yaml
├── go.mod
├── go.sum
└── main.go
```

Содержимое файлов тестового приложения:

??? example "docker-compose.yaml"

	```yaml
	---
	version: '3'

	services:
	picodata-1:
		image: docker-public.binary.picodata.io/picodata:25.2.3
		container_name: picodata-1
		hostname: picodata-1
		environment:
			PICODATA_INSTANCE_NAME: picodata-1
			PICODATA_INSTANCE_DIR: picodata-1
			PICODATA_IPROTO_LISTEN: picodata-1:3301
			PICODATA_IPROTO_ADVERTISE: picodata-1:3301
			PICODATA_PEER: picodata-1:3301
			PICODATA_PG_LISTEN: picodata-1:4327
			PICODATA_PG_SSL: "false"
			PICODATA_ADMIN_PASSWORD: "T0psecret"
		ports:
		- "3301:3301"
		- "4327:4327"

	picodata-2:
		image: docker-public.binary.picodata.io/picodata:25.2.3
		container_name: picodata-2
		hostname: picodata-2
		depends_on:
		- picodata-1
		environment:
			PICODATA_INSTANCE_NAME: picodata-2
			PICODATA_INSTANCE_DIR: picodata-2
			PICODATA_IPROTO_LISTEN: picodata-2:3302
			PICODATA_IPROTO_ADVERTISE: picodata-2:3302
			PICODATA_PG_LISTEN: picodata-2:4328
			PICODATA_PG_SSL: "false"
			PICODATA_PEER: picodata-1:3301
		ports:
		- "3302:3302"
		- "4328:4328"


	picodata-3:
		image: docker-public.binary.picodata.io/picodata:25.2.3
		container_name: picodata-3
		hostname: picodata-3
		depends_on:
		- picodata-1
		environment:
			PICODATA_INSTANCE_NAME: picodata-3
			PICODATA_INSTANCE_DIR: picodata-3
			PICODATA_IPROTO_LISTEN: picodata-3:3303
			PICODATA_IPROTO_ADVERTISE: picodata-3:3303
			PICODATA_PG_LISTEN: picodata-3:4329
			PICODATA_PG_SSL: "false"
			PICODATA_PEER: picodata-1:3301
		ports:
		- "3303:3303"
		- "4329:4329"
	```

??? example "main.go"

	```go
	package main

	import (
		"bufio"
		"context"
		"fmt"
		"math/rand/v2"
		"os"
		"strconv"
		"strings"

		picogo "github.com/picodata/picodata-go"
		"github.com/picodata/picodata-go/logger"
		strats "github.com/picodata/picodata-go/strategies"
	)

	const (
		// In production, you may use environment variable.
		// We expose connection url here to keep example simple.
		CONNECTION_URL = "postgres://admin:T0psecret@localhost:4327"

		// Query to create table to store and get data from.
		QUERY_CREATE_TASKS_TABLE = "create table if not exists tasks (id integer primary key,description text not null)"
	)

	// Pool is a global variable to keep example simple.
	var pool *picogo.Pool

	func main() {
		var err error

		// Connect to the Picodata database.
		pool, err = picogo.New(context.Background(), CONNECTION_URL, picogo.WithBalanceStrategy(strats.NewRoundRobinStrategy()), picogo.WithLogLevel(logger.LevelError))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
			os.Exit(1)
		}
		defer pool.Close()

		// Create test table "tasks" for storing and reading data.
		_, err = pool.Exec(context.Background(), QUERY_CREATE_TASKS_TABLE)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to create table: %v\n", err)
			os.Exit(1)
		}

		reader := bufio.NewReader(os.Stdin)

		// Read user's commands from terminal and execute them.
		for {
			fmt.Print("(db) > ")
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while reading command: %v\n", err)
				continue
			}

			line = strings.TrimSpace(line)

			args := strings.Split(line, " ")

			// Perform CRUD operations.
			switch args[0] {
			case "help":
				printHelp()
				continue
			case "list":
				err = listTasks()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable to list tasks: %v\n", err)
					continue
				}
			case "add":
				err = addTask(strings.Join(args[1:], " "))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable to add task: %v\n", err)
					continue
				}
			case "update":
				n, err := strconv.ParseInt(args[1], 10, 32)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable convert task_num into int32: %v\n", err)
					continue
				}
				err = updateTask(int32(n), strings.Join(args[2:], " "))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable to update task: %v\n", err)
					continue
				}
			case "remove":
				n, err := strconv.ParseInt(args[1], 10, 32)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable convert task_num into int32: %v\n", err)
					continue
				}
				err = removeTask(int32(n))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unable to remove task: %v\n", err)
					continue
				}
			case "exit":
				return
			default:
				fmt.Fprintln(os.Stderr, "Invalid command")
				printHelp()
				continue
			}
		}

	}

	func listTasks() error {
		rows, _ := pool.Query(context.Background(), "select * from tasks")

		for rows.Next() {
			var id int32
			var description string
			err := rows.Scan(&id, &description)
			if err != nil {
				return err
			}
			fmt.Printf("%d -> %s\n", id, description)
		}

		return rows.Err()
	}

	func addTask(description string) error {
		id := rand.Int32()
		_, err := pool.Exec(context.Background(), "insert into tasks values($1, $2)", id, description)
		return err
	}

	func updateTask(itemNum int32, description string) error {
		_, err := pool.Exec(context.Background(), "update tasks set description=$1 where id=$2", description, itemNum)
		return err
	}

	func removeTask(itemNum int32) error {
		_, err := pool.Exec(context.Background(), "delete from tasks where id=$1", itemNum)
		return err
	}

	func printHelp() {
		fmt.Print(`

	Commands:

		help
		list
		add string
		update id string
		remove id
		exit

	Example:

		add 'pico data'
		list

	`)
	}
	```

??? example "go.mod"

	```go
	module picotestapp

	go 1.24.5

	require github.com/picodata/picodata-go v1.0.0

	require (
		github.com/jackc/pgpassfile v1.0.0 // indirect
		github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
		github.com/jackc/pgx/v5 v5.7.2 // indirect
		github.com/jackc/puddle/v2 v2.2.2 // indirect
		golang.org/x/crypto v0.31.0 // indirect
		golang.org/x/sync v0.10.0 // indirect
		golang.org/x/text v0.21.0 // indirect
	)
	```

??? example "go.sum"

	Генерируется автоматически.

<!-- below is the workaround for the linter complaining on ghost broken links -->
[0]: #
[1]: #
[1:]: #
[2:]: #
