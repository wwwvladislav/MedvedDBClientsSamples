import mdv.*;

public class Main {

    public static void main(String[] args) {
        System.loadLibrary("mdv4j");

        mdv.clientInitialize();

        // Conenct to DB
        Client client = Client.connect(new ClientConfig("tcp://localhost:4801"));

        // Create table description
        TableDesc desc = new TableDesc("MyTable");
        desc.addField(FieldType.MDV_FLD_TYPE_CHAR, 0, "Col1");
        desc.addField(FieldType.MDV_FLD_TYPE_INT32, 2, "Col2");
        desc.addField(FieldType.MDV_FLD_TYPE_BOOL, 1, "Col3");

        // Create table
        Table table = client.createTable(desc);
        desc.delete();

        // INSERT

        // Create rows set
        RowSet rowset = new RowSet(table);

        // Fill rows set
        {
            Row row = new Row(3);                                       // Create row

            // Row 1
            {
                row.setString(0, "Hello");                              // First field is string

                // Second field is pair of integers
                ArrayOfInt32 i32arr = new ArrayOfInt32(2);
                i32arr.set(0, 42);
                i32arr.set(1, 43);
                row.setInt32Array(1, i32arr, 2);
                i32arr.delete();

                row.setBool(2, true);                                   // Third field is boolean

                if (!rowset.add(row))                                   // Add row to rows set
                    System.out.println("Row insertion failed");
            }

            // Row 2
            {
                row.setString(0, "World");                              // First field is string
                row.setInt32(1, 44);                                    // Second field is single integer
                row.setBool(2, false);                                  // Third field is boolean
                if (!rowset.add(row))                                   // Add row to rows set
                    System.out.println("Row insertion failed");
            }

            row.delete();                                               // Delete row
        }

        if (!client.insert(rowset))                                     // Insert rows set into the table
            System.out.println("Row insertion into the table failed");

        rowset.delete();                                                // Delete rows set

        // SELECT

        BitSet bitset = new BitSet(3);                                  // Create bit set
        bitset.fill(true);

        RowSet result = client.select(table, bitset, "");               // Request table content

        if (result == null)
            System.out.println("Table content reading failed");

        RowSetEnumerator it = result.enumerator();                      // Get result set iterator

        while(it.next())
        {
            Row row = it.current();

            long f0size = row.fieldSize(0);                             // Get items count in 0's field
            long f1size = row.fieldSize(1);                             // Get items count in 1'st field
            long f2size = row.fieldSize(2);                             // Get items count in 2'nd field

            String f0 = row.getString(0);                               // Get 0's field data
            ArrayOfInt32 f1 = row.getInt32Array(1);                     // Get 1'st field data
            boolean f2 = row.getBool(2);                                // Get 2'nd field data

            row.delete();
        }

        it.delete();                                                    // Delete result set iterator

        result.delete();                                                // Delete result set
        bitset.delete();                                                // Delete bit set
        table.delete();                                                 // Close table
        client.close();                                                 // Close client

        mdv.clientFinalize();
    }
}
