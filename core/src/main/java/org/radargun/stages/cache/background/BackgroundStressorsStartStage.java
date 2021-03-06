package org.radargun.stages.cache.background;

import java.util.List;

import org.radargun.DistStageAck;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.config.TimeConverter;
import org.radargun.stages.AbstractDistStage;
import org.radargun.stages.DefaultDistStageAck;
import org.radargun.stages.helpers.BucketPolicy;

/**
 * 
 * Create BackgroundStressors and store them to SlaveState. Optionally start stressor or stat threads.
 * 
 * @author Michal Linhard &lt;mlinhard@redhat.com&gt;
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Stage(doc = "Starts background stressor threads.")
public class BackgroundStressorsStartStage extends AbstractDistStage {

   @Property(doc = "Ratio of PUT requests. Default is 1.")
   protected int puts = 1;

   @Property(doc = "Ratio of GET requests. Default is 2.")
   protected int gets = 2;

   @Property(doc = "Ratio of REMOVE requests. Default is 0.")
   protected int removes = 0;

   @Property(doc = "Amount of entries (key-value pairs) inserted into the cache. Default is 1024.")
   protected int numEntries = 1024;

   @Property(doc = "Size of value used in the entry. Default is 1024 bytes.")
   protected int entrySize = 1024;

   @Property(doc = "Number of stressor threads. Default is 10.")
   protected int numThreads = 10;

   @Property(doc = "Amount of request wrapped into single transaction. By default transactions are not used (explicitely).")
   protected int transactionSize = -1;

   @Property(converter = TimeConverter.class, doc = "Time between consecutive requests of one stressor thread. Default is 0.")
   protected long delayBetweenRequests = 0;

   @Property(doc = "Specifies whether the stage should wait until the entries are loaded by stressor threads. Default is true.")
   protected boolean waitUntilLoaded = true;

   @Property(doc = "List of slaves where the data should be loaded (others immediately start executing requests). Default is all live slaves).")
   protected List<Integer> loadDataOnSlaves;

   @Property(doc = "List of slaves whose data should be loaded by other threads because these slaves are not alive. Default is empty.")
   protected List<Integer> loadDataForDeadSlaves;

   @Property(doc = "If set to true, the stressor does not execute any requests after loading the data. Default is false.")
   protected boolean loadOnly = false;

   @Property(doc = "Do not execute the loading, start usual request right away.")
   protected boolean noLoading = false;

   @Property(doc = "Use conditional putIfAbsent instead of simple put for loading the keys. Default is false.")
   protected boolean loadWithPutIfAbsent = false;

   @Property(doc = "Use values which trace all operation on these keys. Therefore, they're always growing. Default is false.")
   protected boolean useLogValues = false;

   @Property(doc = "Number of threads on each node that are checking whether all operations from stressor threads have been logged. Default is 10.")
   protected int logCheckingThreads = 10;

   @Property(doc = "Maximum number of records in one entry before the older ones have to be truncated. Default is 100.")
   protected int logValueMaxSize = 100;

   @Property(doc = "Number of operations after which will the stressor or checker update in-cache operation counter. Default is 50.")
   protected long logCounterUpdatePeriod = 50;

   @Property(doc = "Maximum time for which are the log value checkers allowed to show no new checked values " +
         "(error is thrown in CheckBackgroundStressors stage). Default is one minute.", converter = TimeConverter.class)
   protected long logCheckersNoProgressTimeout = 120000;

   @Property(doc = "When the log value is full, the stressor needs to wait until all checkers confirm that " +
         "the records have been checked before discarding oldest records. With ignoreDeadCheckers=true " +
         "the stressor does not wait for checkers on dead nodes. Default is false.")
   protected boolean ignoreDeadCheckers = false;

   @Property(doc = "Period after which a slave is considered to be dead. Default is 90 s.", converter = TimeConverter.class)
   protected long deadSlaveTimeout = 90000;

   @Property(doc = "By default each thread accesses only its private set of keys. This allows all threads all values. " +
         "Atomic operations are required for this functionality. Default is false.")
   protected boolean sharedKeys = false;

   @Property(doc = "Cache used for the background operations. Default is null (default).")
   protected String cacheName;

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      slaveState.put(BucketPolicy.LAST_BUCKET, cacheName);
      try {
         BackgroundOpsManager instance = BackgroundOpsManager.getOrCreateInstance(slaveState, this);

         log.info("Starting stressor threads");
         if (isServiceRunnning()) {
            if (noLoading) {
               instance.setLoaded(true);
            }
            instance.startBackgroundThreads();
            if (waitUntilLoaded) {
               log.info("Waiting until all stressor threads load data");
               instance.waitUntilLoaded();
            }
            instance.setLoaded(true);
         }

         return ack;
      } catch (Exception e) {
         log.error("Error while starting background stats", e);
         ack.setError(true);
         ack.setRemoteException(e);
         return ack;
      }
   }
}
