using System.Text;

namespace Dbhf.File
{
    public class Record
    {
        private const int MaxDataLength = 16;
        public const int Size = 16 + 4 + MaxDataLength;
        public Guid Identifier { get; set; }
        public string Data { get; set; }

        public Record(Guid identifier, string data)
        {
            Identifier = identifier;
            Data = data;
        }

        public Record(int number)
        {
            Identifier = Guid.NewGuid();
            Data = $"Record {number}";
        }

        public override string? ToString()
        {
            return $"{Identifier}: {Data}";
        }

        public byte[] Serialize()
        {
            byte[] idBytes = Identifier.ToByteArray();
            byte[] dataBytes = Encoding.UTF8.GetBytes(Data);
            byte[] dataLength = BitConverter.GetBytes(dataBytes.Length);
            LargestRecordSize = Math.Max(LargestRecordSize, dataBytes.Length);
            byte[] recordBytes = new byte[16 + 4 + MaxDataLength];
            Buffer.BlockCopy(idBytes, 0, recordBytes, 0, idBytes.Length);
            Buffer.BlockCopy(dataLength, 0, recordBytes, 16, 4);
            Buffer.BlockCopy(dataBytes, 0, recordBytes, 20, dataBytes.Length);
            return recordBytes;
        }
        public static int LargestRecordSize
        { get; set; }
    }
}
