namespace Odb.JobRunner;

public interface IProvideCompletableInputs<TInput>
{
    Task<TInput?> GetNextInput();
    Task<TInput> WaitForNextInput(CancellationToken stop);
    Task CompleteInput(TInput input);
    Task HandleExceptionForInput(TInput input, Exception exception);
    Task HandleCanceledInput(TInput input);
}

public interface IHandleInputs<in TInput>
{
    Task Handle(TInput input, CancellationToken stop);
}