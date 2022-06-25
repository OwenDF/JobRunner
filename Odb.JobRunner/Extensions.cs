namespace Odb.JobRunner;

internal static class Extensions
{
    public static (IReadOnlyCollection<T> left, IReadOnlyCollection<T> right) Bifurcate<T>(
        this IEnumerable<T> source,
        Func<T, bool> leftFilter)
    {
        var left = new List<T>();
        var right = new List<T>();
        foreach (var item in source) if (leftFilter(item)) left.Add(item); else right.Add(item);
        return (left, right);
    }
}