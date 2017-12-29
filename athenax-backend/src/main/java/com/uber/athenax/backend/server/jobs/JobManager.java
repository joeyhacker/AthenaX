/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.athenax.backend.server.jobs;

import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.server.InstanceStateUpdateListener;
import com.uber.athenax.backend.server.ServerContext;
import com.uber.athenax.backend.server.yarn.InstanceInfo;
import com.uber.athenax.backend.server.yarn.InstanceManager;
import com.uber.athenax.vm.api.AthenaXTableCatalog;
import com.uber.athenax.vm.api.AthenaXTableCatalogProvider;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import com.uber.athenax.vm.compiler.planner.Planner;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class JobManager implements InstanceStateUpdateListener {
    private static final Logger LOG = LoggerFactory.getLogger(JobManager.class);
    private final InstanceManager instanceManager;
    private final JobStore jobStore;
    private final AthenaXTableCatalogProvider catalogProvider;

    public JobManager(JobStore jobStore, AthenaXTableCatalogProvider catalogProvider, InstanceManager instanceManager) {
        this.jobStore = jobStore;
        this.catalogProvider = catalogProvider;
        this.instanceManager = instanceManager;
    }

    public UUID newJobUUID() {
        return UUID.randomUUID();
    }

    public void deployJob(UUID uuid, JobDefinition job) throws IOException {
        for (JobDefinitionDesiredstate state : job.getDesiredState()) {
            new Thread(() -> {
                try {
                    JobCompilationResult res = compile(job, state);
                    instanceManager.instantiate(state, uuid, res);
                } catch (Throwable ex) {
                    LOG.warn("Failed to instantiate the query '{}' on {}", job.getQuery(), state.getClusterId(), ex);
                }
            }).start();
        }
    }

    public void updateJob(UUID uuid, JobDefinition definition) throws IOException {
        jobStore.updateJob(uuid, definition);
    }

    public void removeJob(UUID uuid) throws IOException {
//        jobStore.removeJob(uuid);
        InstanceInfo instanceInfo = instanceManager.instances().get(uuid);
        try {
            instanceManager.killYarnApplication(instanceInfo.clusterName(), instanceInfo.appId());
        } catch (YarnException e) {
            e.printStackTrace();
        }
    }

    public List<ExtendedJobDefinition> listJobs() throws IOException {
        return jobStore.listAll();
    }

    public JobDefinition getJob(UUID jobUUID) throws IOException {
        return jobStore.get(jobUUID);
    }

    public JobCompilationResult compile(JobDefinition job, JobDefinitionDesiredstate spec) throws Throwable {
        Map<String, AthenaXTableCatalog> inputs = catalogProvider.getInputCatalog(spec.getClusterId());
        AthenaXTableCatalog output = catalogProvider.getOutputCatalog(spec.getClusterId(), job.getOutputs());
        Planner planner = new Planner(inputs, output);
        return planner.sql(job.getQuery(), Math.toIntExact(spec.getResource().getVCores()));
    }


    @Override
    public void onUpdatedInstances(ConcurrentHashMap<UUID, InstanceInfo> instances) {
        try {
            HashMap<UUID, JobDefinition> jobs = JobWatcherUtil.listJobs(jobStore);
            HealthCheckReport report = JobWatcherUtil.computeHealthCheckReport(jobs, instances);
            ServerContext.INSTANCE.watchdogPolicy().onHealthCheckReport(report);
        } catch (IOException e) {
            LOG.warn("Failed to run the health check policy ", e);
        }
    }
}
