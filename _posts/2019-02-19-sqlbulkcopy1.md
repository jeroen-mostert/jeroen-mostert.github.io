---
layout: post
title: The Perils and Pitfalls of SqlBulkCopy (part 1)
---

Eric Lippert has written about the concept of the ["Pit of Quality"](https://blogs.msdn.microsoft.com/ericlippert/2007/08/14/c-and-the-pit-of-despair/) -- designing a language so the rules guide developers towards writing correct code, and actively make it harder to write incorrect code.

[`SqlBulkCopy`](https://docs.microsoft.com/dotnet/api/system.data.sqlclient.sqlbulkcopy) is a class with a lot of design decisions that seem tailor made to prevent developers from falling into the Pit of Quality, and produce decidedly worse results instead. This matters especially because, by its very nature, `SqlBulkCopy` is intended to expose an efficient mechanism for loading lots of data into SQL Server, and yet it seems designed to make sure it actually loads data in the slowest way possible, by virtue of its defaults -- and before you even get to loading the data, there's plenty of pits to fall in.

I should be fair and point out that some of this can be partially attributed to the defaults picked by the bulk insert mechanism of SQL Server itself. Nevertheless, it's `SqlBulkCopy` which chooses to expose these defaults as-is, instead of choosing more sensible ones. This is a kind of "laziness" you often see when developers are tasked with writing classes that wrap functionality already present in another system. In some cases, there is something to say for keeping the wrapper simple and one-to-one, so developers can use the existing documentation and knowledge and map this easily onto the new way. I don't think that applies to `SqlBulkCopy`, however, and there's a better design struggling to get out.

So in which way does `SqlBulkCopy` fail? First, let's set up a very simple bulk load scenario, which is admittedly simplified but not unrepresentative of what a developer would be tasked to do. Let's say we have a table `Widgets` as follows:

```sql
CREATE TABLE Widgets(
    [ID] INT IDENTITY PRIMARY KEY, 
    [Name] NVARCHAR(100), 
    [WeightInGrams] INT
)
```
    
Let's also say we've got a file that contains a load of widget definitions from elsewhere that looks like this:

```
1	azultronic radiator	2.60
12	boson catcher	0.43
15	nuclear accelerator	10.0
21	screw	0.045
...
```

In this file, the columns are separated by tabs, and the weight is sneakily given in kilograms, rather than grams, just so our application has something to do and you couldn't just import this with a wizard. The ID has been given to us from another database, and even though it's an identity, we'd like to preserve it because, for better or worse, we've come to depend on these values. (Isn't that just how these things go?)

There are several ways of tackling this issue that don't involve writing any .NET code at all:
* We could import the file using [`bcp`](https://docs.microsoft.com/sql/tools/bcp-utility), [`BULK INSERT`](https://docs.microsoft.com/sql/t-sql/statements/bulk-insert-transact-sql) or [`OPENROWSET(BULK, ...)`](https://docs.microsoft.com/sql/t-sql/functions/openrowset-transact-sql#using-openrowset-with-the-bulk-option) and write some T-SQL code to fix up the weight.
* We could use an [SSIS package](https://docs.microsoft.com/sql/integration-services/sql-server-integration-services) to perform the necessary load/transformation option.
* We could use some other third party ETL tool, like Pentaho's [Kettle](https://github.com/pentaho/pentaho-kettle).

These solutions all have their pros and cons, and for a simple scenario like this, it probably doesn't matter what you do: pick whatever solution you can develop the quickest, depending on what you have the most experience with. For the sake of argument, let's say I've decided to give pure .NET a go, because I'm unwilling or unable to port my existing logic to a completely new environment.

Let's first cook up some code to read the file. I'm going to use the simplest thing that could possibly work, as that's usually a nice place to start...

```csharp
class Widget {
    public int Id { get; set; }
    public string Name { get; set; }
    public int WeightInGrams { get; set; }
}

IEnumerable<Widget> ReadFromFile(string fileName) {
    foreach (var line in File.ReadLines(fileName)) {
        string[] fields = line.Split('\t');
        yield return new Widget {
            Id = int.Parse(fields[0], CultureInfo.InvariantCulture),
            Name = fields[1],
            WeightInGrams = (int) (decimal.Parse(fields[2], CultureInfo.InvariantCulture) * 1000)
        };
    }
}
```

This code is quite naive, having no way to nicely handle input errors, but let's look past that for the moment and figure out how to get it imported into our table.

Here we hit our first speed bump. The method in `SqlBulkCopy` that actually performs the work is `WriteToServer`, which has the following overloads:

```csharp
WriteToServer(DataTable, DataRowState)
WriteToServer(IDataReader)
WriteToServer(DataTable)
WriteToServer(DbDataReader)
WriteToServer(DataRow[])
```
    
Since implementing `IDataReader` looks like quite a bit of work, and `DataRow` has no public constructors, it looks like `DataTable` is the way to go. We could re-tool our existing code to directly read things in a `DataTable`, but let's say I like the current separation of concerns and would rather have this as a separate step. Writing our bulk import code is a simple affair:

```csharp
var dt = new DataTable();
dt.Columns.Add("Id");
dt.Columns.Add("Name");
dt.Columns.Add("WeightInGrams");
foreach (var widget in ReadFromFile(@"widgets.txt")) {
    var row = dt.NewRow();
    row["Id"] = widget.Id;
    row["Name"] = widget.Name;
    row["WeightInGrams"] = widget.WeightInGrams;
    dt.Rows.Add(row);
}
using (var connection = new SqlConnection("Data Source=.;Initial Catalog=Widgets;Integrated Security=SSPI")) {
    connection.Open();
    using (var sqlBulkCopy = new SqlBulkCopy(connection)) {
        sqlBulkCopy.DestinationTableName = "WidgetView";
        sqlBulkCopy.WriteToServer(dt);
    }
}
```

If we run this code, our resulting table will end up like this:

| ID |        Name         | WeightInGrams |
|----|---------------------|---------------|
|  1 | azultronic radiator |          2600 |
|  2 | boson catcher       |           430 |
|  3 | nuclear accelerator |         10000 |
|  4 | screw               |            45 |

Wait, what happened to our `ID`? Well, remember that `ID` is an `IDENTITY`. By default, any values you supply will simply be ignored in favor of the generated `IDENTITY`.

Here I'd like to make a little aside -- suppose this is exactly what you wanted, and the input file had no `ID`. How would you perform the bulk copy in that case? It would seem obvious to simply leave it out and do this:

```csharp
var dt = new DataTable();
dt.Columns.Add("Name");
dt.Columns.Add("WeightInGrams");
foreach (var widget in ReadFromFile(@"widgets.txt")) {
    var row = dt.NewRow();
    row["Name"] = widget.Name;
    row["WeightInGrams"] = widget.WeightInGrams;
    dt.Rows.Add(row);
}
```
        
Alright, let's try that:

| ID | Name  | WeightInGrams |
|----|-------|---------------|
|  1 |  2600 | NULL          |
|  2 |   430 | NULL          |
|  3 | 10000 | NULL          |
|  4 |    45 | NULL          |

Wait, what? How did we get this bizarre result? Well, unless you explicitly tell it otherwise, `SqlBulkCopy` will map input to output columns in the physical order they occur in your input and the table. This is true even if you supply your columns with names in a `DataTable`.

This is obviously the Wrong Thing to do. If the fact that we gave it column names wasn't enough, depending on the physical order of columns in a database table is a Bad Idea -- this order doesn't logically matter, and about the only place where you can see it is if you do a `SELECT *`. The fix isn't hard, all we have to do is add the following:

```csharp
foreach (DataColumn column in dt.Columns) {
    sqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
}
```

But the fact that we have to add this at all seems clumsy, to say the least.

Back to our original scenario -- we want our supplied `ID` to be respected. This requires an explicit option:

```csharp
using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, null)) {
```
    
Why is this an option you have to pass to the constructor, instead of a property, or an argument you can pass to `WriteToServer`? Well, um, it just is -- no reason I can think of. We now also have to pass an explicit `null` for `SqlTransaction`, even though we're not using that. So now we run that, and:

> Unhandled Exception: System.InvalidOperationException: The given ColumnMapping does not match up with any column in the source or destination.
    
What now? How did we displease the `SqlBulkCopy` gods this time? I'll just come out and tell you, since you could waste quite a bit of time trying to puzzle it out otherwise: in our table, the column is spelled `ID`, with a capital `D`, but in our `DataTable` we've used `Id`, to match the property name. If you're like a lot of people, your database will use a case-insensitive collation, so you don't normally notice such mismatches. `SqlBulkCopy` performs an ordinal comparison, however, and does not care for sloppiness. Still, you would think it could be nice and *tell* us which columns, exactly, fail to map -- but no such luck. (And note how it did not complain when the `KeepIdentity` option was not supplied, because it ignored the whole column in the first place!)

If we fix our column spelling, we finally obtain the desired result:

| ID |        Name         | WeightInGrams |
|----|---------------------|---------------|
|  1 | azultronic radiator |          2600 |
| 12 | boson catcher       |           430 |
| 15 | nuclear accelerator |         10000 |
| 21 | screw               |            45 |

Great! Are we done? Well, as long as we're content with importing only four rows into an empty table, yes, we're done. That, however, is hardly what we need `SqlBulkCopy` for -- what about inserting *lots* of data? 
