Cicada
======

Dependably fast multi-core in-memory transactions

Requirements
------------

 * Linux x86\_64 >= 3.0
 * Intel CPU >= Haswell
 * Hugepage (2 GiB) support

Dependencies for compilation
----------------------------

 * g++ >= 5.3
 * cmake >= 2.8
 * make >= 3.81
 * libnuma-dev >= 2.0

Dependencies for execution
--------------------------

 * bash >= 4.0
 * python >= 3.4

Compiling Cicada
----------------

         * cd cicada-core/build
         * cmake ..
         * make -j

Setting up the general environment
----------------------------------

         * cd cicada-core/build
         * ln -s src/mica/test/*.json .
         * ../script/setup.sh 16384 16384    # 2 NUMA nodes, 32 Ki pages (64 GiB)

Running microbench
------------------

         * cd cicada-core/build
         * sudo ./test_tx 10000000 16 0.95 0.99 200000 28

Note
----
 * The main namespace is mica for historical reasons.  This may change in the future.
 * Some code (e.g., memory pool allocation) needs to be modified for many-core (> 64 cores) non-dual-socket systems.
 * NUMA-aware parts are tested on a dual-socket system that assigns even-numbered lcore IDs to CPU 0 cores and odd-numbered lcore IDs to CPU 1 cores.
 * The system expects a full memory bandwidth configuration (e.g., all 4 channels are active).
 * Busy-waiting in contention regulation can be inefficient if hyperthreading is enabled.
   * StaticConfig::kPairwiseSleeping can be enabled to reduce wasted cycles on hyperthreading (experimental).
 * Backoff is currently using only RDTSC for spinning, which can add an excessive delay upon VM live migration.

Authors
-------

Hyeontaek Lim (hl@cs.cmu.edu)

License
-------

        Copyright 2014, 2015, 2016, 2017 Carnegie Mellon University

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.

