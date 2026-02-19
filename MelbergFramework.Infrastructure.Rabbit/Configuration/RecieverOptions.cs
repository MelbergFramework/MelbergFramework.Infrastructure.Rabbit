using System.ComponentModel.DataAnnotations;

namespace MelbergFramework.Infrastructure.Rabbit.Configuration;

public class RecieverOptions
{
    [Required]
    public string Name { get; set; }
    [Required]
    public string Connection { get; set; }
    public string Queue { get; set; }
    public string Exchange { get; set; }
    [Range(1, int.MaxValue)]
    public int Scale { get; set; } = 1;
}
