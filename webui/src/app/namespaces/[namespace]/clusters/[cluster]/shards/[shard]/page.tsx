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

import { Container, Typography, Tooltip } from "@mui/material";
import { ShardSidebar } from "@/app/ui/sidebar";
import { fetchShard } from "@/app/lib/api";
import { useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { AddShardCard, CreateCard } from "@/app/ui/createCard";
import Link from "next/link";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";

const truncateText = (text: string, limit: number) => {
    return text.length > limit ? `${text.slice(0, limit)}...` : text;
};

export default function Shard({
    params,
}: {
    params: { namespace: string; cluster: string; shard: string };
}) {
    const { namespace, cluster, shard } = params;
    const [nodesData, setNodesData] = useState<any>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNodes = await fetchShard(namespace, cluster, shard);
                if (!fetchedNodes) {
                    console.error(`Shard ${shard} not found`);
                    router.push("/404");
                    return;
                }
                setNodesData(fetchedNodes);
            } catch (error) {
                console.error("Error fetching shard data:", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [namespace, cluster, shard, router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    return (
        <div className="flex h-full">
            <ShardSidebar namespace={namespace} cluster={cluster} />
            <Container
                maxWidth={false}
                disableGutters
                sx={{ height: "100%", overflowY: "auto", marginLeft: "16px" }}
            >
                <div className="flex flex-row flex-wrap">
                    <AddShardCard namespace={namespace} cluster={cluster} />
                    {nodesData.nodes.map(
                        (node: any, index: number) => (
                            <Link
                                href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${index}`}
                                key={index}
                            >
                                <CreateCard>
                                    <Typography variant="h6" gutterBottom>
                                        Node {index + 1}
                                    </Typography>
                                    <Tooltip title={node.id}>
                                        <Typography
                                            variant="body2"
                                            gutterBottom
                                            sx={{
                                                whiteSpace: "nowrap",
                                                overflow: "hidden",
                                                textOverflow: "ellipsis",
                                            }}
                                        >
                                            ID: {truncateText(node.id, 20)}
                                        </Typography>
                                    </Tooltip>
                                    <Typography variant="body2" gutterBottom>
                                        Address: {node.addr}
                                    </Typography>
                                    <Typography variant="body2" gutterBottom>
                                        Role: {node.role}
                                    </Typography>
                                    <Typography variant="body2" gutterBottom>
                                        Created At: {new Date(node.created_at * 1000).toLocaleString()}
                                    </Typography>
                                </CreateCard>
                            </Link>
                        )
                    )}
                </div>
            </Container>
        </div>
    );
}
