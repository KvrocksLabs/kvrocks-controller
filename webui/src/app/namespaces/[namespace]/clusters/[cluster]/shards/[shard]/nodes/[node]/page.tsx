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

import { listNodes } from "@/app/lib/api";
import { NodeSidebar } from "@/app/ui/sidebar";
import {
    Box,
    Container,
    Card,
    Alert,
    Snackbar,
    Typography,
    Tooltip,
} from "@mui/material";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { AddNodeCard, CreateCard } from "@/app/ui/createCard";
import { truncateText } from "@/app/utils";

export default function Node({
    params,
}: {
  params: { namespace: string; cluster: string; shard: string; node: string };
}) {
    const { namespace, cluster, shard, node } = params;
    const router = useRouter();
    const [nodeData, setNodeData] = useState<any>(null);
    const [loading, setLoading] = useState<boolean>(true);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNodes = await listNodes(namespace, cluster, shard);
                if (!fetchedNodes) {
                    console.error(`Shard ${shard} not found`);
                    router.push("/404");
                    return;
                }
                setNodeData(fetchedNodes);
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
            <NodeSidebar namespace={namespace} cluster={cluster} shard={shard} />
            <Container
                maxWidth={false}
                disableGutters
                sx={{ height: "100%", overflowY: "auto", marginLeft: "16px" }}
            >
                <div className="flex flex-row flex-wrap">
                    {nodeData.map((nodeObj: any, index: number) =>
                        index === Number(node) ? (
                            <>
                                <CreateCard>
                                    <Typography variant="h6" gutterBottom>
                    Node {index + 1}
                                    </Typography>
                                    <Tooltip title={nodeObj.id}>
                                        <Typography
                                            variant="body2"
                                            gutterBottom
                                            sx={{
                                                whiteSpace: "nowrap",
                                                overflow: "hidden",
                                                textOverflow: "ellipsis",
                                            }}
                                        >
                      ID: {truncateText(nodeObj.id, 20)}
                                        </Typography>
                                    </Tooltip>
                                    <Typography variant="body2" gutterBottom>
                    Address: {nodeObj.addr}
                                    </Typography>
                                    <Typography variant="body2" gutterBottom>
                    Role: {nodeObj.role}
                                    </Typography>
                                    <Typography variant="body2" gutterBottom>
                    Created At:{" "}
                                        {new Date(nodeObj.created_at * 1000).toLocaleString()}
                                    </Typography>
                                </CreateCard>
                            </>
                        ) : null
                    )}
                </div>
            </Container>
        </div>
    );
}
