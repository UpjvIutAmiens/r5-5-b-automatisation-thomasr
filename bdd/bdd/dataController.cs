using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace bdd;

[ApiController]
[Route("api/[controller]")]
public class DataController : ControllerBase
{
    private readonly IConfiguration _configuration;

    public DataController(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    [HttpPost("insert-data")]
    public async Task<IActionResult> InsertData([FromBody] DataPacket packet)
    {
        var connectionString = _configuration.GetConnectionString("Database");
        
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        int inserted = 0;
        int skipped = 0;

        foreach (var record in packet.Data)
        {
            if (await RecordExists(connection, record))
            {
                skipped++;
                continue;
            }

            await InsertRecord(connection, record);
            inserted++;
        }

        return Ok(new { inserted, skipped });
    }

    private async Task<bool> RecordExists(NpgsqlConnection conn, CsvRecord record)
    {
        // Your duplicate check logic
        var cmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM your_table WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", record.Id);
        
        var count = (long)(await cmd.ExecuteScalarAsync() ?? 0);
        return count > 0;
    }

    private async Task InsertRecord(NpgsqlConnection conn, CsvRecord record)
    {
        // Your insert logic
        var cmd = new NpgsqlCommand(
            "INSERT INTO your_table (id, name, value) VALUES (@id, @name, @value)", 
            conn);
        cmd.Parameters.AddWithValue("id", record.Id);
        cmd.Parameters.AddWithValue("name", record.Name);
        cmd.Parameters.AddWithValue("value", record.Value);
        
        await cmd.ExecuteNonQueryAsync();
    }
}

public record DataPacket(List<CsvRecord> Data);
public record CsvRecord(int Id, string Name, string Value);