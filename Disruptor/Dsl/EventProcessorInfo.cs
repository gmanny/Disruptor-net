using System.Threading;
using System.Threading.Tasks;

namespace Disruptor.Dsl
{
    internal class EventProcessorInfo<T> : IConsumerInfo
    {
        private bool endOfChain = true;

        public EventProcessorInfo(IEventProcessor eventProcessor, IEventHandler<T> eventHandler, ISequenceBarrier sequenceBarrier)
        {
            EventProcessor = eventProcessor;
            EventHandler = eventHandler;
            Barrier = sequenceBarrier;
        }

        public IEventProcessor EventProcessor { get; private set; }
        public IEventHandler<T> EventHandler { get; private set; }
        public ISequenceBarrier Barrier { get; private set; }

        public Sequence[] Sequences
        {
            get { return new[] { EventProcessor.Sequence }; }
        }

        public bool IsEndOfChain
        {
            get { return endOfChain; }
        }

        public void Start(TaskScheduler scheduler)
        {
            Task.Factory.StartNew(EventProcessor.Run, CancellationToken.None, TaskCreationOptions.LongRunning, scheduler);
        }

        public void Halt()
        {
            EventProcessor.Halt();
        }

        public void MarkAsUsedInBarrier()
        {
            endOfChain = false;
        }
    }
}
