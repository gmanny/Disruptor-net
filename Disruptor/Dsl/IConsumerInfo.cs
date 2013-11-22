using System.Threading.Tasks;

namespace Disruptor.Dsl
{
    internal interface IConsumerInfo
    {
        Sequence[] Sequences { get; }

        ISequenceBarrier Barrier { get; }

        bool IsEndOfChain { get; }

        void Start(TaskScheduler scheduler);

        void Halt();

        void MarkAsUsedInBarrier();
    }
}