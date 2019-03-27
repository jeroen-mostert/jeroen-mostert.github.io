---
layout: post
title: The Perils and Pitfalls of SqlBulkCopy (part 2)
---

[When we last saw our (peculiarly anonymous) heroes](/sqlbulkcopy1), they had just managed to do a very basic bulk import using `SqlBulkCopy`, despite some initial hurdles figuring out the finer points. As a reminder, here's the code we ended up with:

```csharp
var dt = new DataTable();
dt.Columns.Add("ID");
dt.Columns.Add("Name");
dt.Columns.Add("WeightInGrams");
foreach (var widget in ReadFromFile("widgets.txt")) {
    var row = dt.NewRow();
    row["ID"] = widget.Id;
    row["Name"] = widget.Name;
    row["WeightInGrams"] = widget.WeightInGrams;
    dt.Rows.Add(row);
}

using (var connection = new SqlConnection("Data Source=.;Initial Catalog=Widgets;Integrated Security=SSPI")) {
    connection.Open();
    using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, null)) {
        foreach (DataColumn column in dt.Columns) {
            sqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        }
        sqlBulkCopy.DestinationTableName = "Widgets";
        sqlBulkCopy.WriteToServer(dt);
    }
}
```

This code works fine for a file containing 4 rows. What about a file with 10 million rows? That's where it gets a little more interesting. First, let's make a little script for generating such a file:

```csharp
var r = new Random(42);
using (var f = File.OpenWrite(@"superwidgets.txt"))
using (var w = new StreamWriter(f)) {
    int id = 1;
    for (int i = 0; i != 10_000_000; ++i) {
        string name = "azultronic radiator " + i;
        string weight = "2.60";
        w.WriteLine($"{id}\t{name}\t{weight}");
        id += r.Next(5) + 1;
    }
}
```

That's a lot of azultronic radiators, which may not be completely representative of our widget inventory, but it's a good start.

So how does our little bulk insert program fare with this data set? If we add some very simple performance logging with `Stopwatch` and calls to `GC.GetTotalMemory()`/`GC.GetAllocatedBytesForCurrentThread()`, I get the following:

```
Reading rows     : 32.06 s
Memory (total)   : 7 102 941 056
Memory (current) : 3 417 498 648
```

> Unhandled Exception: System.Data.SqlClient.SqlException: Timeout expired.  The timeout period elapsed prior to completion of the operation or the server is not responding. ---> System.ComponentModel.Win32Exception: The wait operation timed out.

Defaults strike again! `.BulkCopyTimeout` starts off at 30 seconds. A reasonable value for run of the mill SQL commands, but rather arbitrary for bulk inserts, which can easily take longer, of course -- we're usually more interested in whether the copy is making sufficient progress, not how long it takes in total. No fear, we can set the timeout to `0`, indicating we're willing to wait for however long it takes. So let's do that and try again.

```
Reading rows     : 33.34 s
Memory (total)   : 7 102 942 536
Memory (current) : 3 417 510 576
Inserting rows   : 47.96 s
Total time       : 86.88 s
```

The exact numbers shouldn't concern us a lot, here -- and I'm intentionally not mentioning my machine specs, so as to not give the impression this is a representative benchmark. Suffice it to say it has a lot of memory, the disks are all SSD and the database files have been pre-sized, so this is pretty much a best case scenario.

What's immediately striking is that we spend 30 seconds and a whopping 7 GB in allocated memory (of which 3.5 GB still present at the end) just reading the widgets, before anything is sent to SQL Server at all. This is a direct consequence of the fact that we're using `DataTable`, which, by virtue of having to be able to store rows of any format, is quite the memory hog. There isn't a whole lot we can do, here. We can reduce the memory use by about 1 GB by specifying the types of the columns in advance:

```csharp
dt.Columns.Add("ID", typeof(int));
dt.Columns.Add("Name", typeof(string));
dt.Columns.Add("WeightInGrams", typeof(int));
```

Result:

```
Reading rows     : 25.15 s
Memory (total)   : 6 123 159 104
Memory (current) : 2 567 739 576
```

A measurable improvement, but not an order of magnitude. For that, we'll have to ditch `DataTable` altogether. Recall that `.WriteToServer` had more overloads. The one we should be interested in is this one:

```csharp
WriteToServer(IDataReader)
```

`IDataReader` is a bit of a weird interface: it implements `IDataRecord`, which represents a single record of data, and adds `.Read()` to actually advance to the next record. `SqlDataReader` ultimately implements this to represent row sets read from a SQL Server.

The key insight here is that `IDataReader` never has to make more than one row at a time available -- in effect it's just like `IEnumerable`, but with the ability to address columns individually. In fact, we can just make an implementation that wraps `IEnumerable`:

```csharp
class ObjectReader<T> : IDataReader {
    private readonly IEnumerator<T> sequence;
    private readonly PropertyInfo[] properties;
    private readonly Dictionary<string, PropertyInfo> propertiesByName;

    public ObjectReader(IEnumerable<T> sequence) {
        this.sequence = sequence.GetEnumerator();
        this.properties = typeof(T).GetProperties();
        this.propertiesByName = properties.ToDictionary(p => p.Name, p => p);
    }

    public object this[int i] => properties[i].GetValue(sequence.Current);
    public object this[string name] => propertiesByName[name].GetValue(sequence.Current);

    public int Depth => 0;
    public bool IsClosed { get; private set; }
    public int RecordsAffected => 0;
    public int FieldCount => properties.Length;

    public void Close() => IsClosed = true;

    public void Dispose() => sequence.Dispose();

    public bool GetBoolean(int i) => (bool) this[i];
    public byte GetByte(int i) => (byte) this[i];
    // and many more like this...

    public string GetDataTypeName(int i) => this[i].GetType().Name;
    public Type GetFieldType(int i) => this[i].GetType();
    public string GetName(int i) => properties[i].Name;
    public int GetOrdinal(string name) => Array.FindIndex(properties, p => p.Name == name);
    public object GetValue(int i) => this[i];

    public int GetValues(object[] values) {
        for (int i = 0; i != properties.Length; ++i) {
            values[i] = this[i];
        }
        return properties.Length;
    }

    public bool IsDBNull(int i) => this[i] == null || this[i] == DBNull.Value;
    public bool NextResult() => false;
    public bool Read() => sequence.MoveNext();
}
```

This is pretty obviously not the safest or most efficient implementation of this concept, but ignore that for the moment. We can now change our program to:

```csharp
using (var objectReader = new ObjectReader<Widget>(ReadFromFile("superwidgets.txt")))
using (var connection = new SqlConnection("Data Source=.;Initial Catalog=Widgets;Integrated Security=SSPI")) {
    connection.Open();
    using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, null)) {
        foreach (string name in typeof(Widget).GetProperties().Select(p => p.Name)) {
            sqlBulkCopy.ColumnMappings.Add(name, name);
        }
        sqlBulkCopy.DestinationTableName = "Widgets";
        sqlBulkCopy.BulkCopyTimeout = 0;
        sqlBulkCopy.WriteToServer(objectReader);
    }
}
```

And now our performance characteristics look quite different:

```
Memory (total)   : 4 157 254 328
Memory (current) : 160 840
Inserting rows   : 48.17
Total time       : 48.17
```

There is no more time for "reading rows", since the reading is now part of the copy process, and we also make a lot less demand on memory.

So, is this it? Is this the fastest possible way to fill the table? Not by a long shot, but that wasn't really the point -- the point was to show that `SqlBulkCopy` could do a lot more to give you a good experience out of the box. There is still more it could be doing, and we'll look at that in part 3. Spoilers: this is going to involve changing code in the framework itself, so it'll be quite fun!
