using Dbhf.File;
using System.Diagnostics;

namespace Dbhf
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int defaultRecords = 1_000_000;
            Console.WriteLine($"Creating file with {defaultRecords} records, records per block {HeapFile.RecordsPerBlock}, block size {HeapFile.BlockSize} B.");
            Stopwatch st = new Stopwatch();
            st.Start();
            var actualFile = HeapFile.Create(totalRecords: defaultRecords);
            Console.WriteLine($"File created in {st.ElapsedMilliseconds} ms.");

            Console.WriteLine("Cumulative writes: ");
            using (var writer = new HeapFile.Writer(HeapFile.FileName))
            {
                for (int i = 0; i < 1024; i++)
                {
                    var record = new Record(i);
                    writer.Write(new[] { record });
                }
            }
            Console.WriteLine($"----------------------------------------------");
            Console.WriteLine("Reading records: ");
            Console.WriteLine("Single buffer implementation (press any key to begin): ");
            Console.ReadKey(intercept: true);
            st.Restart();
            IList<Record> records;
            using (var reader = new HeapFile.Reader(actualFile, useDualBuffering: false))
                records = reader.ReadAll().ToList();
            st.Stop();
            var singleBufferTime = st.ElapsedMilliseconds;
            Console.WriteLine($"----------------------------------------------");
            Console.WriteLine($"Anticipatory reads (press any key to begin): ");
            Console.ReadKey(intercept: true);
            st.Restart();
            IList<Record> records_double;
            using (var reader = new HeapFile.Reader(actualFile))
                records_double = reader.ReadAll().ToList();
            st.Stop();

            var doubleBufferTime = st.ElapsedMilliseconds;
            Console.WriteLine($"Single buffer read: Read {records.Count} records in {singleBufferTime} ms.");
            Console.WriteLine($"Double buffer read: Read {records_double.Count} records in {doubleBufferTime} ms.");
            Console.WriteLine("Press any key to pring records: ");
            Console.ReadKey(intercept: true);
            foreach (var record in records_double)
                Console.WriteLine(record);
        }

    }
}