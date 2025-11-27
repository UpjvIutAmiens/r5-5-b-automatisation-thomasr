var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddControllers();

var app = builder.Build();

app.MapControllers();
app.Run("http://localhost:8080/");