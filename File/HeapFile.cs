using System.Text;

namespace Dbhf.File
{
    public class HeapFile
    {
        private const int HeaderSize = 8;
        public const int RecordsPerBlock = 1_000;
        public const int BlockSize = RecordsPerBlock * Record.Size;
        public const string FileName = "heapfile.bin";
        public static string Create(string? filepath = null, int totalRecords = 1_000_000)
        {
            if (string.IsNullOrEmpty(filepath))
                filepath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), FileName);

            byte[] blockBuffer = new byte[BlockSize];
            int currentRecord = 0;

            using (FileStream fs = new FileStream(filepath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                1,
                FileOptions.WriteThrough))
            using (BinaryWriter bw = new BinaryWriter(fs))
            {
                var numberOfBlocks = (Int32)Math.Ceiling((double)totalRecords / RecordsPerBlock);
                bw.Write(numberOfBlocks);
                bw.Write(BlockSize);

                while (currentRecord < totalRecords)
                {
                    int recordIndex = 0;
                    while (recordIndex < RecordsPerBlock && currentRecord < totalRecords)
                    {
                        Record record = new Record(currentRecord);
                        byte[] recordBytes = record.Serialize();
                        Buffer.BlockCopy(recordBytes, 0, blockBuffer, recordIndex * Record.Size, Record.Size);
                        recordIndex++;
                        currentRecord++;
                    }
                    bw.Write(blockBuffer, 0, Record.Size * recordIndex);
                }
            }

            return filepath;
        }
        public class Reader : IDisposable
        {
            private readonly object _sync = new object();

            private byte[][] _buffers;
            private int _activeBuffer = 0;
            private int[] _readLengths;
            private Task<int>[] _loadTasks;

            private int _blockSize = -1;
            private int _numberOfBlocks = -1;
            private bool _useDualBuffer;

            private FileStream _fs;
            private BinaryReader _br;

            public Reader(string filepath, bool useDualBuffering = true)
            {
                _useDualBuffer = useDualBuffering;
                _fs = new FileStream(filepath, FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    1,
                    FileOptions.SequentialScan);
                _br = new BinaryReader(_fs);
                ProcessHeader();
                _buffers = new byte[_useDualBuffer ? 2 : 1][];
                _loadTasks = new Task<int>[_useDualBuffer ? 2 : 1];
                _readLengths = new int[_useDualBuffer ? 2 : 1];

                for (int i = 0; i < _buffers.Length; i++)
                {
                    _buffers[i] = new byte[_blockSize];
                    _loadTasks[i] = LoadIntoBuffer(i);
                    // ugly
                    _loadTasks[i].Wait();
                }
            }

            private Task<int> LoadIntoBuffer(int bufferIndex)
            {
                if (_fs.Position == _fs.Length)
                {
                    return Task.FromResult(0);
                }
                Console.WriteLine($"Fetching data into buffer {bufferIndex}");
                return Task.Run(() =>
                {
                    int bytesRead = 0;
                    lock (_sync)
                        bytesRead = _br.Read(_buffers[bufferIndex], 0, _blockSize);
                    Console.WriteLine($"Loaded {bytesRead} bytes into buffer {bufferIndex}");
                    return bytesRead;
                });
            }

            private void ProcessHeader()
            {
                _numberOfBlocks = _br.ReadInt32();
                _blockSize = _br.ReadInt32();
                Console.WriteLine($"Header processed:\n\tBlock Size = {_blockSize} B,\n\tNumber of Blocks = {_numberOfBlocks}");
            }

            public IEnumerable<Record> ReadAll()
            {
                int position = 0;

                while (true)
                {
                    int activeBuffer = _activeBuffer;

                    _loadTasks[activeBuffer].Wait();
                    _readLengths[activeBuffer] = _loadTasks[activeBuffer].Result;

                    Console.WriteLine($"Processing buffer {activeBuffer}...");
                    while (position < _readLengths[activeBuffer])
                    {
                        if (position + Record.Size > _readLengths[activeBuffer])
                        {
                            break;
                        }
                        var record = FromBytes(_buffers[activeBuffer].AsSpan(position, Record.Size));
                        position += Record.Size;
                        yield return record;
                    }
                    Console.WriteLine($"Finished processing buffer {activeBuffer}.");
                    if (_fs.Position <= _fs.Length)
                    {
                        // Finished processing active buffer, begin fetching data
                        // optionally swap to secondary buffer, which should have been loaded in the meantime
                        _loadTasks[activeBuffer] = LoadIntoBuffer(activeBuffer);
                        if (_useDualBuffer)
                        {
                            _activeBuffer = (_activeBuffer + 1) % 2;
                            Console.WriteLine($"Swapping data buffer to {_activeBuffer}");
                        }
                        else
                        {
                            Console.WriteLine("Fetching data...");
                        }
                    }

                    position = 0;
                    if (_readLengths[activeBuffer] == 0)
                    {
                        break;
                    }
                }
            }

            private Record FromBytes(ReadOnlySpan<byte> data)
            {
                Guid id = new Guid(data.Slice(0, 16));
                int length = BitConverter.ToInt32(data.Slice(16, 4));
                var bytes = data.Slice(20, data.Length - 20);
                string text = Encoding.UTF8.GetString(bytes).TrimEnd('\0');
                return new Record(id, text);
            }
            public void Dispose()
            {
                _fs?.Dispose();
                _br?.Dispose();
            }
        }
        public class Writer : IDisposable
        {
            private byte[] _buffer = new byte[BlockSize];
            private int _bufferedRecords = 0;
            private int _position = 0;
            private FileStream _fs;
            private BinaryWriter _bw;

            public Writer(string filepath)
            {
                _fs = new FileStream(filepath, FileMode.Create, FileAccess.Write, FileShare.None, 1, FileOptions.WriteThrough);
                _bw = new BinaryWriter(_fs);
            }
            public void Write(IEnumerable<Record> records, bool flush = false)
            {
                int blockWrites = 0;
                foreach (var record in records)
                {
                    if (_position + Record.Size > BlockSize)
                    {
                        Console.WriteLine("Writing block...");
                        blockWrites++;
                        _bw.Write(_buffer, 0, _bufferedRecords * Record.Size);
                        _bufferedRecords = 0;
                        _position = 0;
                    }
                    var serialized = record.Serialize();
                    Buffer.BlockCopy(serialized, 0, _buffer, _position, serialized.Length);
                    _position += serialized.Length;
                    _bufferedRecords++;
                }
                if (flush && _bufferedRecords > 0)
                {
                    _bw.Write(_buffer, 0, _bufferedRecords * Record.Size);
                }
                if (_bufferedRecords > 0)
                {
                    Console.WriteLine($"Buffer: {_position} B -> {_bufferedRecords} records");
                }
            }

            public void Flush()
            {
                if (_bufferedRecords > 0)
                {
                    Console.WriteLine("Writer releasing resources, flushing buffer to disk.");
                    Console.WriteLine($"Writing {_bufferedRecords} records.");
                    _bw.Write(_buffer, 0, _bufferedRecords * Record.Size);
                    _bufferedRecords = 0;
                    _position = 0;
                }
            }

            public void Dispose()
            {
                Flush();
                _fs.Dispose();
            }
        }
    }
}
