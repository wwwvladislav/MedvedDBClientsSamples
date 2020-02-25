import mdv.*;

public class Main {

    public static void main(String[] args) {
        System.loadLibrary("mdv4j");

        mdv.clientInitialize();

        // Conenct to DB
        Client client = Client.connect(new ClientConfig("tcp://localhost:4801"));

        // Create table description
        TableDesc desc = new TableDesc("MyTable");
        assert(desc.addField(FieldType.MDV_FLD_TYPE_CHAR, 0, "Col1"));
        assert(desc.addField(FieldType.MDV_FLD_TYPE_INT32, 2, "Col2"));
        assert(desc.addField(FieldType.MDV_FLD_TYPE_BOOL, 1, "Col3"));

        // Create table
        Table table = client.createTable(desc);
        desc.delete();

        // Create rows set
        RowSet rowset = new RowSet(3);

        // Fill rows set
        {
            // Create row
            Row row = new Row(3);

            // Row 1
            {
                assert(row.set(0, "Hello"));        // First field is string

                // Second field is pair of integers
                ArrayOfInt32 i32arr = new ArrayOfInt32(2);
                i32arr.set(0, 42);
                i32arr.set(1, 43);
                assert(row.set(1, i32arr, 2));
                i32arr.delete();

                assert(row.set(2, true));           // Third field is boolean

                assert(rowset.add(row));            // Add row to rows set
            }

            // Row 2
            {
                assert(row.set(0, "World"));        // First field is string
                assert(row.set(1, 44));             // Second field is single integer
                assert(row.set(2, false));          // Third field is boolean
                assert(rowset.add(row));            // Add row to rows set
            }

            row.delete();                           // Delete row
        }

        assert(client.insertRows(table, rowset));   // Insert rows set into the table

        rowset.delete();                            // Delete rows set

        table.close();                              // Close table

        client.close();                             // Close client

        mdv.clientFinalize();
    }
}
