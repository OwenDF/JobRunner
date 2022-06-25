namespace Odb.JobRunner;

using static Task;

public class JobRunner<TInput>
{
    private readonly IProvideCompletableInputs<TInput> _inputs;
    private readonly IHandleInputs<TInput> _handler;
    private readonly int _maxConcurrentInputs;

    public JobRunner(IProvideCompletableInputs<TInput> inputs, IHandleInputs<TInput> handler, Settings settings)
    {
        _inputs = inputs;
        _handler = handler;
        _maxConcurrentInputs = settings.MaxConcurrentInputs;
    }

    public Task Run() => Run(CancellationToken.None);

    public async Task Run(CancellationToken stop)
    {
        List<Job> jobs = new List<Job>();

        while (!stop.IsCancellationRequested)
        {
            jobs = await FillWithJobs(jobs, stop);
            if (!jobs.Any())
            {
                var next = await _inputs.WaitForNextInput(stop);
                jobs.Add(new(_handler.Handle(next, stop), next));
                if (stop.IsCancellationRequested) break;
                continue;
            }

            await WhenAny(jobs.Select(x => x.Running));
            jobs = await FinaliseCompletedJobs(jobs);
        }

        await WhenAll(jobs.Select(x => x.Running));
        await FinaliseCompletedJobs(jobs);
    }

    private async Task<List<Job>> FinaliseCompletedJobs(List<Job> currentJobs)
    {
        var jobs = currentJobs.Bifurcate(x => x.Running.IsCompleted);

        foreach (var completedJob in jobs.left)
        {
            if (completedJob.Running.IsFaulted)
            {
                await _inputs.HandleExceptionForInput(completedJob.Input, completedJob.Running.Exception!);
                completedJob.Running.Dispose();

            }
            else if (completedJob.Running.IsCanceled)
            {
                await _inputs.HandleCanceledInput(completedJob.Input);
                completedJob.Running.Dispose();
            }
            else
                await _inputs.CompleteInput(completedJob.Input);
        }

        return jobs.right.ToList();
    }

    private async Task<List<Job>> FillWithJobs(List<Job> existingJobs, CancellationToken stop)
    {
        var next = await _inputs.GetNextInput();

        while (next is not null)
        {
            existingJobs.Add(new(_handler.Handle(next, stop), next));
            
            if (existingJobs.Count == _maxConcurrentInputs) return existingJobs;
            next = await _inputs.GetNextInput();
        }

        return existingJobs;
    }

    public record Settings(int MaxConcurrentInputs);
    private record Job(Task Running, TInput Input);
}
