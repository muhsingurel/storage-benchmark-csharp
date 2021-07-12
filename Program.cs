using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace storage_benchmark
{

    //Usage:
    //  storage-benchmark-csharp[options]

    //Options:
    //  --file-path<file-path> filePath
    //  --offset-option<offset-option> offsetOption[default: -1]
    //  --parallel-option parallelOption[default: False]
    //  -?, -h, --help Show help and usage information

    class Program
    {
        /// <param name="filePath">filePath for the read test. A file larger than 100GB is recommended.</param>
        /// <param name="offsetOption">An optional offset number as number of chunks between 0-10000. Otherwise it is random</param>
        /// <param name="parallelOption">Optianal parallel read argument. --parallelOption true for parallel read</param>
        /// <param name="dataChunkSize">Optianal data chunk size argument between 1024-262144(256KB) default is 10000</param>
        static void Main(string filePath, int offsetOption = -1, bool parallelOption = false, int dataChunkSize = 10000)
        {
            Console.WriteLine("----Storage Benchmark----");
            Console.WriteLine("");

            //Ensure that the dataChunkSize is between 1024-131072 otherwise set it to min or max
            dataChunkSize = dataChunkSize < 1024 ? 1024 : dataChunkSize;
            dataChunkSize = dataChunkSize > 128 * 1024 ? 256 * 1024 : dataChunkSize;

            //Initialize a stopwatch to measure performance
            var stopwatch = new Stopwatch();

            long fileLenght = 0;
            long checksum = 0;

            //Get total lenght of the file, we will use this to calculate number of chunks to read. using statement will dispose the resource automatically.
            using (FileStream streamSource = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fileLenght = streamSource.Length;
            }

            // Each chunck size is 10000 bytes by default otherwise parameter input
            int singleChunkBufferLenght = dataChunkSize;
            // We can read these chunks from 10000 different location
            long maxOffsetAsChunks = 10000;
            int numberOfChunksToRead = (int)(fileLenght / (maxOffsetAsChunks * (long)singleChunkBufferLenght));

            // If offsetOption is not provided with arguments, make it random. Otherwise input which is between [0-10000)
            if (offsetOption < 0)
            {
                Random random = new Random();
                offsetOption = random.Next(0, 10000);
            }
            else
            {
                // Guarantees the offset is in range
                offsetOption %= 10000;
            }

            // Default is false which is sequential read test, if true make a parallel read test
            if (parallelOption)
            {
                // Parallel read test
                stopwatch.Start();
                // We need an array so that each tread can access to a different index without blocking other threads.
                // If we use the global checksum variable, it may cause race condition.
                // The best way is to store each trace result in a different checksumBuffer index
                long[] checksumBuffer = new long[numberOfChunksToRead];
                Parallel.For(0, numberOfChunksToRead, chunkIndex =>
                {
                    // Start reading the chunks and calculate chunk sum as a checksum. Later we can verify if the file is actually read with other tools.
                    // Each itteration creates a different file handler. When the itteration is complete the resource is disposed. Each itteration calculates the chunk position.
                    using (FileStream streamSource = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, singleChunkBufferLenght, FileOptions.Asynchronous))
                    {
                        byte[] singleChunkBuffer = new byte[singleChunkBufferLenght];
                        streamSource.Position = ((long)maxOffsetAsChunks * (long)chunkIndex * (long)singleChunkBuffer.Length + ((long)singleChunkBuffer.Length * (long)offsetOption)) % fileLenght;
                        streamSource.Read(singleChunkBuffer, 0, singleChunkBuffer.Length);
                        //Calculate the chunk sum.
                        for (int i = 0; i < singleChunkBuffer.Length; i++)
                        {
                            checksumBuffer[chunkIndex] += (long)singleChunkBuffer[i];
                        }
                        Console.WriteLine("Offset value: " + offsetOption + " Chunk Number: " + chunkIndex + " Position: " + streamSource.Position + " Filelength: " + fileLenght);
                    }
                });
                stopwatch.Stop();
                Console.WriteLine("Data chunk size: " + singleChunkBufferLenght + " Checksum: " + checksumBuffer.Sum() + "   Parallel read operation took " + stopwatch.ElapsedMilliseconds + " ms ");
                return;
            }
            else
            {
                // Sequential read test
                stopwatch.Start();
                byte[] singleChunkBuffer = new byte[singleChunkBufferLenght];

                // Start reading the chunks and calculate chunk sum as a checksum. Later we can verify if the file is actually read with other tools.
                // Create a single file handler. With each itteration change the positon to the next chunk to read.
                using (FileStream streamSource = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, singleChunkBufferLenght, FileOptions.Asynchronous))
                {
                    for (int chunkIndex = 0; chunkIndex < numberOfChunksToRead; chunkIndex++)
                    {
                        streamSource.Position = ((long)maxOffsetAsChunks * (long)chunkIndex * (long)singleChunkBuffer.Length + ((long)singleChunkBuffer.Length * (long)offsetOption)) % fileLenght;
                        streamSource.Read(singleChunkBuffer, 0, singleChunkBuffer.Length);
                        //Calculate the chunk sum.
                        for (int i = 0; i < singleChunkBuffer.Length; i++)
                        {
                            checksum += (long)singleChunkBuffer[i];
                        }
                        Console.WriteLine("Offset value: " + offsetOption + " Chunk Number: " + chunkIndex + " Position: " + streamSource.Position + " Filelength: " + fileLenght);
                    }
                }
                stopwatch.Stop();
                Console.WriteLine("Data chunk size: " + singleChunkBufferLenght + " Checksum: " + checksum + "   Sequential read operation took " + stopwatch.ElapsedMilliseconds + " ms ");
                return;
            }
        }
    }
}
