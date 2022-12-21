global using RabbitMQ.Stream.Client;
global using System.Net;
global using Microsoft.Extensions.Options;
using SuperStreamClients;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSwarmSuperStream(
    builder.Configuration.GetSection(RabbitMqStreamOptions.Name),
    options =>
    {
    });

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
