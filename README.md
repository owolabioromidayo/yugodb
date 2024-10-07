# YugoDB

<div align="center">
  <img src="https://github.com/owolabioromidayo/yugodb/assets/37741645/1c4ea88d-92fe-4540-a4e0-ceba1d04ac34" alt="YugoDB architecture diagram" width="200">
</div>

融合<strong>(Yūgō)</strong> -> <strong>Fusion</strong>

The database engine to fuse them all. Relational/Document and Row/Column oriented storage at your fingertips, with support for cross-type joins.

## Features
- [X] Custom Query Language and Interpreter
- [X] Vector materialization model for query execution
- [X] Multi-file paging system and cache
- [X] Ser/deser of relational and document row DBs
- [X] Thousand row insertion
- [X] DBMS operations through query string ( CreateDB, CreateTable, Insert)
- [X] Insert document and specify relational schemas using json strings in query
- [X] 1k entries test 
## In Progress
- [ ] B+ Tree range queries and ser/deser V+ Tree into pages
- [ ] Million row challenge and culling hotpaths
- [ ] Batched operations
- [ ] More extensive testing and refactor
- [ ] Hook up client to new changes,  test and optimize concurrency features
- [ ] Database state serialization and WAL
- [ ] Index Loop Joins

## Future Plans
- [ ] Optimization model, might require rewrite



## Example Usage

```
        // Setup db and tables
        dbs.create_db('test_db');
        dbs.create_table('test_db' ,'test_table', 'DOCUMENT', 'ROW');

        dbs.create_table('test_db' ,'test_rtable', 'RELATIONAL', 'ROW', '{
            'name': 'string(50)',
            'balance': ['numeric', true],
            'pob': 'string',
            'active': 'boolean'
        }');

        //insert values
        dbs.insert('test_db', 'test_table', '{ 
                'name': 'John Doe',
                'age': 30.0,
                'city': 'New York',
                'address': {
                    'street': '123 Main St',
                    'zip': '10001'
                },
                'phone_numbers': [
                    '123-456-7890',
                    '987-654-3210'
                ]
        }');

        dbs.insert('test_db', 'test_table', '{
            'name': 'Jane Smith',
            'age': 25.0,
            'city': 'London',
            'address': {
                'street': '456 High St',
                'zip': 'SW1A 1AA'
            },
            'phone_numbers': [
                '020-1234-5678'
            ],
            'employment': {
                'company': 'Acme Inc.',
                'position': 'Software Engineer',
                'start_date': {
                'year': 2022.0,
                'month': 1.0
                }
            }
            }');

            dbs.insert('test_db', 'test_rtable', '{
                'name': 'Jane Smith',
                'balance': '2502034304.2332',
                'pob': 'London',
                'active': true
            }');

            dbs.insert('test_db', 'test_rtable', '{
                'name': 'John Doe',
                'balance': '450.2332',
                'pob': 'New York',
                'active': false
            }');
       
        //actual dataflow
        let x = dbs.test_db.test_table.offset(0);  
        let y = dbs.test_db.test_rtable.offset(0);  
        let z  = x LJOIN y ON name=name;
        z.limit(10);

```
<b>
More documentation will be provided on the execution model and internals soon!   
</b
