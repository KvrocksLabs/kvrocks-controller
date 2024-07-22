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

import { Box, Container, Card, Typography } from "@mui/material";
import { ClusterSidebar } from "../../../../ui/sidebar";
import { AddClusterCardProps, CreateCard } from "../../../../ui/createCard";
import { fetchClusters } from "@/app/lib/api";
import Link from "next/link";

export default async function Cluster({
    params,
}: {
  params: { namespace: string; cluster: string };
}) {
    const clusters = await fetchClusters(params.namespace);
    return (
        <div className="flex h-full">
            <ClusterSidebar namespace={params.namespace} />
            <Container
                maxWidth={false}
                disableGutters
                sx={{ height: "100%", overflowY: "auto", marginLeft: "16px" }}
            >
                <div className="flex flex-row flex-wrap">
                    This is the cluster page
                </div>
            </Container>
        </div>
    );
}
