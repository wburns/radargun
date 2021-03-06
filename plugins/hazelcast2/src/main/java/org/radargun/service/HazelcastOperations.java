package org.radargun.service;

import com.hazelcast.core.IMap;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.BasicOperations;
import org.radargun.traits.ConditionalOperations;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class HazelcastOperations implements BasicOperations, ConditionalOperations {
   protected final static Log log = LogFactory.getLog(HazelcastOperations.class);
   protected final static boolean trace = log.isTraceEnabled();

   protected final HazelcastService service;

   public HazelcastOperations(HazelcastService service) {
      this.service = service;
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      return new Cache<K, V>((IMap<K,V>) service.hazelcastInstance.getMap(cacheName));
   }

   protected class Cache<K, V> implements BasicOperations.Cache<K, V>, ConditionalOperations.Cache<K, V> {
      protected final IMap<K, V> map;

      public Cache(IMap<K, V> map) {
         this.map = map;
      }

      @Override
      public V get(K key) {
         return map.get(key);
      }

      @Override
      public boolean containsKey(K key) {
         return map.containsKey(key);
      }

      @Override
      public void put(K key, V value) {
         map.put(key, value);
      }

      @Override
      public V getAndPut(K key, V value) {
         return map.put(key, value);
      }

      @Override
      public boolean remove(K key) {
         return map.remove(key) != null;
      }

      @Override
      public V getAndRemove(K key) {
         return map.remove(key);
      }

      @Override
      public boolean replace(K key, V value) {
         return map.replace(key, value) != null;
      }

      @Override
      public V getAndReplace(K key, V value) {
         return map.replace(key, value);
      }

      @Override
      public void clear() {
         map.clear();
      }

      @Override
      public boolean putIfAbsent(K key, V value) {
         return map.putIfAbsent(key, value) == null;
      }

      @Override
      public boolean remove(K key, V oldValue) {
         return map.remove(key, oldValue);
      }

      @Override
      public boolean replace(K key, V oldValue, V newValue) {
         return map.replace(key, oldValue, newValue);
      }
   }
}
