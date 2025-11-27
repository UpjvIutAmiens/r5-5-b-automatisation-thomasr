using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using System.Threading.Tasks;

class Program
{
    static void Main(string[] args)
    {
        int port = 6000;
        var server = new TcpListener(IPAddress.Any, port);

        server.Start();
        Console.WriteLine($"[BDD] Listening on port {port}");

        while (true)
        {
            var client = server.AcceptTcpClient();
            Console.WriteLine("[BDD] Client connected");

            Task.Run(() => HandleClient(client));
        }
    }

    static async Task HandleClient(TcpClient client)
    {
        var stream = client.GetStream();

        try
        {
            // --------------------------   
            // 1. Read header (length)
            // --------------------------
            string header = await ReadLine(stream);
            int expectedLength = int.Parse(header);

            // --------------------------
            // 2. Read exactly "expectedLength" bytes
            // --------------------------
            byte[] buffer = new byte[expectedLength];
            int totalRead = 0;

            while (totalRead < expectedLength)
            {
                int read = await stream.ReadAsync(buffer, totalRead, expectedLength - totalRead);
                if (read == 0) throw new Exception("Client disconnected unexpectedly");
                totalRead += read;
            }

            string chunk = Encoding.UTF8.GetString(buffer);
            Console.WriteLine("[BDD] Received chunk:");
            Console.WriteLine(chunk);

            // --------------------------
            // 3. Process CSV lines
            // --------------------------
            bool success = await ProcessChunk(chunk);

            // --------------------------
            // 4. Send response
            // --------------------------
            string response = success ? "SUCCESS" : "ERROR";
            byte[] respBytes = Encoding.UTF8.GetBytes(response);
            await stream.WriteAsync(respBytes);

            Console.WriteLine("[BDD] Sent back: " + response);
        }
        catch (Exception ex)
        {
            Console.WriteLine("[BDD] ERROR: " + ex.Message);
            byte[] respBytes = Encoding.UTF8.GetBytes("ERROR");
            await stream.WriteAsync(respBytes);
        }
        finally
        {
            client.Close();
        }
    }

    static async Task<string> ReadLine(NetworkStream stream)
    {
        StringBuilder sb = new StringBuilder();

        while (true)
        {
            int b = stream.ReadByte();
            if (b == -1) throw new Exception("Client disconnected");
            if (b == '\n') break;
            sb.Append((char)b);
        }

        return sb.ToString().Trim();
    }

    // ----------------------------------------------------------------------
    // PROCESSING LOGIC: Check if row exists, insert if not
    // ----------------------------------------------------------------------
    static async Task<bool> ProcessChunk(string chunk)
    {
        string[] lines = chunk.Split('\n', StringSplitOptions.RemoveEmptyEntries);

        string connectionString =
            "Host=db;Port=5432;Username=postgres;Password=postgres;Database=mydb";

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        foreach (string line in lines)
        {
            try
            {
                // Example CSV format: id,name,value
                string[] cols = line.Split(',');

                string id = cols[0];
                string name = cols[1];
                string value = cols[2];

                // Check existing entry
                await using (var checkCmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM mytable WHERE id = @id", conn))
                {
                    checkCmd.Parameters.AddWithValue("id", id);
                    long count = (long)await checkCmd.ExecuteScalarAsync();

                    if (count == 0)
                    {
                        // Insert new row
                        await using (var insertCmd = new NpgsqlCommand(
                            "INSERT INTO mytable (id, name, value) VALUES (@id, @name, @value)",
                            conn))
                        {
                            insertCmd.Parameters.AddWithValue("id", id);
                            insertCmd.Parameters.AddWithValue("name", name);
                            insertCmd.Parameters.AddWithValue("value", value);
                            await insertCmd.ExecuteNonQueryAsync();
                        }

                        Console.WriteLine("[BDD] Inserted: " + line);
                    }
                    else
                    {
                        Console.WriteLine("[BDD] Already exists: " + line);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[BDD] ERROR processing line: " + line);
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        return true;
    }
}
