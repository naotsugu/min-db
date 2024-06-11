# Min Database


## How to run

```console
$ ./gradlew jar
$ java -jar ./app/build/libs/app.jar
```

## run sql

```console
Connect(jdbc:min-db:test)> jdbc:min-db:example
Creating new database
transaction 1 committed

SQL> create table persons(sid int, lastname varchar(20), firstname varchar(20), age int)
transaction 2 committed
0 records processed

SQL> insert into persons(sid, lastname, firstname, age) values (1, 'Baker', 'Kyle', 28)
transaction 3 committed
1 records processed

SQL> insert into persons(sid, lastname, firstname, age) values (2, 'Morris', 'Morgan', 31)
transaction 4 committed
1 records processed

SQL> insert into persons(sid, lastname, firstname, age) values (3, 'Smith', 'Noel', 46)
transaction 5 committed
1 records processed

SQL> insert into persons(sid, lastname, firstname, age) values (4, 'Lucas', 'Wil', 37)
transaction 6 committed
1 records processed

SQL> select lastname, firstname from persons
             lastname            firstname
------------------------------------------
                Baker                 Kyle
               Morris               Morgan
                Smith                 Noel
                Lucas                  Wil
transaction 8 committed

SQL> exit
transaction 9 committed
```

