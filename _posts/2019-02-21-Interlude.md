---
layout: post
title: Interlude: the Perils and Pitfalls of `BULK INSERT`
---

As I've been [ranting against `SqlBulkCopy`](/sqlbulkcopy1), here's a brief interlude about how not *everything* is its fault.

Consider our little table again:

```sql
CREATE TABLE Widgets(
    [ID] INT IDENTITY PRIMARY KEY, 
    [Name] NVARCHAR(100), 
    [WeightInGrams] INT
)
```

Every bulk insert mechanism that's not `SqlBulkCopy` offers a way to specify the order of the rows you're inserting using the `ORDER` hint. The purpose of this is to speed up inserting rows into a table with a clustered index, which otherwise forces an intermediate sort step -- which can be very, very costly when you're inserting big batches.

Unfortunately, there's one crucial scenario where this `ORDER` hint will do nothing: if the clustered index is on a column with the `IDENTITY` property (which it is, in the above table, since the primary key implicitly becomes the clustered index unless we specify otherwise). In this case, SQL Server will sort the incoming rows no matter what -- it will not even give an error if you're not supplying the rows in order, as you're supposed to.

The irony here (if you can call it that) is that this sort will be omitted if you do *not* supply a value for the identity column. In this case, SQL Server will insert the rows any which way, and generate identity values on its own. That's great if you're inserting new values; not so much if you're copying from an existing source.

It turns out this is a [very old problem](https://feedback.azure.com/forums/908035-sql-server/suggestions/32895409), that just happens to have not been fixed yet. There is a workaround, of sorts (it can't be universally applied in all cases): `ALTER TABLE SWITCH`, which does not care about `IDENTITY`. We can create the following table:

```sql
CREATE TABLE _BulkWidgets(
    [ID] INT PRIMARY KEY, 
    [Name] NVARCHAR(100), 
    [WeightInGrams] INT
)
```

We can bulk insert into that without running into this bug (I think we can call it a bug) and then issue the following statement:

```sql
ALTER TABLE _BulkWidgets SWITCH TO Widgets;
```

And you probably want to follow that up with `DBCC CHECKIDENT(Widgets, RESEED)`.

For this to work `Widgets` must be empty, so this doesn't help if you want to bulk insert into a table that already contains data. If your table isn't empty, you can still use this technique, but only if you're willing to live without your data while the bulk insert is happening: you can `ALTER TABLE Widgets SWITCH TO _BulkWidgets`, bulk insert, then switch back. It's possible to stuff all these operations (switch, bulk insert, switch) into a transaction, so clients don't see an empty table, and the whole thing would be rolled back as one. Nevertheless, this still makes your data temporarily unavailable, so we can only hope that Microsoft sees fit to fix this.
