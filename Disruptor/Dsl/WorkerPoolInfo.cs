using System.Threading.Tasks;

namespace Disruptor.Dsl
{
    internal class WorkerPoolInfo<T> : IConsumerInfo where T : class
    {
        private readonly WorkerPool<T> workerPool;
        private readonly ISequenceBarrier sequenceBarrier;
        private bool endOfChain = true;

        public WorkerPoolInfo(WorkerPool<T> workerPool, ISequenceBarrier sequenceBarrier)
        {
            this.workerPool = workerPool;
            this.sequenceBarrier = sequenceBarrier;
        }

        public Sequence[] Sequences
        {
            get { return workerPool.WorkerSequences; }
        }

        public ISequenceBarrier Barrier
        {
            get { return sequenceBarrier; }
        }

        public bool IsEndOfChain
        {
            get { return endOfChain; }
        }

        public void Start(TaskScheduler scheduler)
        {
            workerPool.Start(scheduler);
        }

        public void Halt()
        {
            workerPool.Halt();
        }

        public void MarkAsUsedInBarrier()
        {
            endOfChain = false;
        }
    }
}