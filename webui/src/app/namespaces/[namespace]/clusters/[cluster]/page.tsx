/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

"use client";

import {
    Box,
    Container,
    Typography
} from "@mui/material";
import { ClusterSidebar } from "../../../../ui/sidebar";
import { useState, useEffect } from "react";
import { listShards } from "@/app/lib/api";
import { AddShardCard, CreateCard } from "@/app/ui/createCard";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";

export default function Cluster({
    params,
}: {
  params: { namespace: string; cluster: string };
}) {
    const { namespace, cluster } = params;
    const [shardsData, setShardsData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedShards = await listShards(namespace, cluster);

                if (!fetchedShards) {
                    console.error(`Shards not found`);
                    router.push("/404");
                    return;
                }

                setShardsData(fetchedShards);
            } catch (error) {
                console.error("Error fetching shards:", error);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, [namespace, cluster, router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    return (
        <div className="flex h-full">
            <ClusterSidebar namespace={namespace} />
            <Container
                maxWidth={false}
                disableGutters
                sx={{ height: "100%", overflowY: "auto", marginLeft: "16px" }}
            >
                <div className="flex flex-row flex-wrap">
                    <AddShardCard namespace={namespace} cluster={cluster} />
                    {shardsData.map((shard, index) => (
                        <Link
                            key={index}
                            href={`/namespaces/${namespace}/clusters/${cluster}/shards/${index}`}
                        >
                            <CreateCard>
                                <Typography variant="h6" gutterBottom noWrap>
                  Shard {index + 1}
                                </Typography>
                                <Typography variant="body2" gutterBottom>
                  Nodes : {shard.nodes.length}
                                </Typography>
                                <Typography variant="body2" gutterBottom>
                  Slots: {shard.slot_ranges.join(", ")}
                                </Typography>
                                <Typography variant="body2" gutterBottom>
                  Target Shard Index: {shard.target_shard_index}
                                </Typography>
                                <Typography variant="body2" gutterBottom>
                  Migrating Slot: {shard.migrating_slot}
                                </Typography>
                            </CreateCard>
                        </Link>
                    ))}
                </div>
            </Container>
        </div>
    );
}
