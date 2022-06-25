namespace Odb.JobRunner;

using static Task;

public class JobRunner<TInput> : IRunAsync
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

    public async Task Run(CancellationToken stop)
    {
        var jobs = new List<Job>(_maxConcurrentInputs);

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
        var (left, right) = currentJobs.Bifurcate(x => x.Running.IsCompleted);

        foreach (var (running, input) in left)
        {
            if (running.IsFaulted)
            {
                await _inputs.HandleExceptionForInput(input, running.Exception!);
                running.Dispose();

            }
            else if (running.IsCanceled)
            {
                await _inputs.HandleCanceledInput(input);
                running.Dispose();
            }
            else
                await _inputs.CompleteInput(input);
        }

        return right.ToList();
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
