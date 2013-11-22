using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Disruptor.Dsl
{
    internal class ConsumerRepository<T> where T : class
    {
        public class IdentityEqualityComparer<TKey> : IEqualityComparer<TKey> where TKey : class
        {
            public int GetHashCode(TKey value)
            {
                return RuntimeHelpers.GetHashCode(value);
            }

            public bool Equals(TKey left, TKey right)
            {
                return ReferenceEquals(left, right); // Reference identity comparison
            }
        }

        private readonly IDictionary<IEventHandler<T>, EventProcessorInfo<T>> _eventProcessorInfoByHandler = 
            new Dictionary<IEventHandler<T>, EventProcessorInfo<T>>(new IdentityEqualityComparer<IEventHandler<T>>());
        private readonly IDictionary<Sequence, IConsumerInfo> _eventProcessorInfoBySequence =
            new Dictionary<Sequence, IConsumerInfo>(new IdentityEqualityComparer<Sequence>());
        private readonly ICollection<IConsumerInfo> _consumerInfos = new List<IConsumerInfo>();

        public void Add(IEventProcessor eventProcessor, IEventHandler<T> eventHandler, ISequenceBarrier sequenceBarrier)
        {
            var eventProcessorInfo = new EventProcessorInfo<T>(eventProcessor, eventHandler, sequenceBarrier);
            _eventProcessorInfoByHandler[eventHandler] = eventProcessorInfo;
            _eventProcessorInfoBySequence[eventProcessor.Sequence] = eventProcessorInfo;
            _consumerInfos.Add(eventProcessorInfo);
        }

        public void Add(IEventProcessor processor)
        {
            var eventProcessorInfo = new EventProcessorInfo<T>(processor, null, null);
            _eventProcessorInfoBySequence[processor.Sequence] = eventProcessorInfo;
            _consumerInfos.Add(eventProcessorInfo);
        }

        public void Add(WorkerPool<T> workerPool, ISequenceBarrier sequenceBarrier)
        {
            WorkerPoolInfo<T> workerPoolInfo = new WorkerPoolInfo<T>(workerPool, sequenceBarrier);
            _consumerInfos.Add(workerPoolInfo);
            foreach (Sequence sequence in workerPool.WorkerSequences)
            {
                _eventProcessorInfoBySequence[sequence] = workerPoolInfo;
            }
        }

        public Sequence[] LastSequenceInChain
        {
            get
            {
                List<Sequence> result = new List<Sequence>();

                foreach (IConsumerInfo consumerInfo in _consumerInfos)
                {
                    if (consumerInfo.IsEndOfChain)
                    {
                        result.AddRange(consumerInfo.Sequences);
                    }
                }

                return result.ToArray();
            }
        }

        public IEventProcessor GetEventProcessorFor(IEventHandler<T> eventHandler)
        {
            EventProcessorInfo<T> eventProcessorInfo;
            if (!_eventProcessorInfoByHandler.TryGetValue(eventHandler, out eventProcessorInfo))
            {
                throw new ArgumentException("The event handler " + eventHandler + " is not processing events.");
            }

            return eventProcessorInfo.EventProcessor;
        }

        public Sequence GetSequenceFor(IEventHandler<T> handler)
        {
            return GetEventProcessorFor(handler).Sequence;
        }


        public void UnmarkEventProcessorsAsEndOfChain(params Sequence[] barrierEventProcessors)
        {
            foreach (var barrierEventProcessor in barrierEventProcessors)
            {
                GetEventProcessorInfo(barrierEventProcessor).MarkAsUsedInBarrier();
            }
        }

        public IEnumerable<IConsumerInfo> EventProcessors
        {
            get { return _consumerInfos; }
        }

        public ISequenceBarrier GetBarrierFor(IEventHandler<T> handler)
        {
            var eventProcessorInfo = GetEventProcessorInfo(handler);
            return eventProcessorInfo != null ? eventProcessorInfo.Barrier : null;
        }

        private EventProcessorInfo<T> GetEventProcessorInfo(IEventHandler<T> handler)
        {
            return _eventProcessorInfoByHandler[handler];
        }

        private IConsumerInfo GetEventProcessorInfo(Sequence barrierEventProcessor)
        {
            return _eventProcessorInfoBySequence[barrierEventProcessor];
        }
    }
}
