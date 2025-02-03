# Relational-Core

## <a name="embedded"></a>The Embedded JDBC Driver: `url=jdbc:embed:/__SYS?schema=CATALOG`
Relational core bundles a JDBC Driver that offers direct-connect on a Relational instance.
The Driver class name, `com.apple.foundationdb.relational.jdbc.JDBCRelationalDriver`, is registered with
[Service Loader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html).

JDBC URLs have a `jdbc:embed:` prefix. An URL that connects to the Relational
system database, `__SYS`, using the system database `CATALOG` would look like this:
```
jdbc:embed:/__SYS?schema=CATALOG
```
Connecting to the `/FRL/YCSB` database using the `ycsb` schema:
```
jdbc:embed:/FRL/YCSB?schema=YCSB
```

There is no one 'fat' convenience driver jar that bundles all dependencies built
currently. Access to the fdb client native library complicates our being able to
deliver a single `driver` jar (TODO).

The [fdb-relational-cli](../fdb-relational-cli/README.md) comes with the embedded Driver bundled.

Relational also bundles a JDBC client Driver that talks GRPC to a Relational
Server/Service. See [Relational JDBC client Driver](../fdb-relational-jdbc/README.md)
for more.

## Development Environment Setup

### Syntax highlighting of YAML test files ###
To activate SQL syntax highlighting for YAMSQL files, do the following:
- Locate your IntelliJ's [configuration location](https://www.jetbrains.com/help/idea/directories-used-by-the-ide-to-store-settings-caches-plugins-and-logs.html#config-directory). It should be
  something like `~/Library/Application Support/JetBrains/IdeaIC2023.2`.
- Navigate to `filetypes` folder (or create it if it does not exist), then copy the following script into a new file called `YAML-SQL.xml`.
- Restart IntelliJ.

```xml
<filetype binary="false" default_extension="yamsql" description="YAML SQL Test files (syntax highlighting only)" name="YAML-SQL">
  <highlighting>
    <options>
      <option name="LINE_COMMENT" value="#" />
      <option name="COMMENT_START" value="" />
      <option name="COMMENT_END" value="" />
      <option name="HEX_PREFIX" value="" />
      <option name="NUM_POSTFIXES" value="" />
      <option name="HAS_BRACES" value="true" />
      <option name="HAS_BRACKETS" value="true" />
      <option name="HAS_PARENS" value="true" />
      <option name="HAS_STRING_ESCAPES" value="true" />
      <option name="LINE_COMMENT_AT_START" value="false" />
    </options>
    <keywords keywords="absolute;action;add;all;allocate;alter;and;any;are;array;as;asc;assertion;at;authorization;
    avg;begin;between;bit_length;both;by;cascade;cascaded;case;cast;catalog;character_length;check;close;coalesce;
    collate;collation;column;commit;connection;constraint;constraints;continue;convert;corresponding;count;create;
    cross;current;current_;current_date;current_time;current_timestamp;cursor;database;day;deallocate;declare;default;
    deferrable;deferred;delete;desc;describe;descriptor;diagnostics;disconnect;distinct;domain;drop;else;end;escape;
    except;exception;exec;execute;exists;external;extract;fetch;first;for;foreign;found;from;full;get;global;go;goto;
    grant;group;having;hour;identity;if;immediate;in;index;indicator;initially;inner;input;insensitive;insert;intersect;
    interval;into;is;isolation;join;key;language;last;leading;left;length;level;like;limit;local;lower;match;max;min;
    minute;module;month;names;national;natural;next;no;not;nullif;octet_length;of;on;only;open;option;or;order;outer;
    output;overlaps;pad;partial;position;precision;prepare;preserve;primary;prior;privileges;procedure;public;read;
    references;relative;restrict;revoke;right;rollback;rows;schema;scroll;second;section;select;session;session_;set;
    recursive;references;relative;restrict;revoke;right;rollback;rows;schema;scroll;second;section;select;session;session_;set;
    size;some;space;sql;sqlcode;sqlerror;sqlstate;struct;substring;sum;system_user;table;template;temporary;then;
    timezone_;timezone_minute;to;trailing;transaction;translate;translation;trim;type;union;unique;unknown;update;
    upper;usage;user;using;value;values;varying;view;when;whenever;where;with;work;write;year;zone" ignore_case="true" />
    <keywords2 keywords="bigint;bit;boolean;bytes;char;char_;character;date;dec;decimal;double;float;int;integer;nchar;
    numeric;real;smallint;string;time;timestamp;varchar;!!;!r;!in;!a;" />
    <keywords3 keywords="false;null;true;ordered;randomized;parallelized;test;block;single_repetition_ordered;
    single_repetition_randomized;single_repetition_parallelized;multi_repetition_ordered;multi_repetition_randomized;
    multi_repetition_parallelized" />
    <keywords4 keywords="connect:;query:;load schema template:;set schema state:;result:;unorderedresult:;explain:;
    explaincontains:;count:;error:;planhash:;setup:;schema_template:;test_block:;options:;tests:;mode:;repetition:;seed:;
    check_cache:;connection_lifecycle:;steps:;preset:;statement_type:;!r;!in;!a;" />
  </highlighting>
  <extensionMap>
    <mapping ext="yamsql" />
  </extensionMap>
</filetype>
```
