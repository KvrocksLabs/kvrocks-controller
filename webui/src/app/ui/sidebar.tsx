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

'use client';

import { Divider, List } from "@mui/material";
import { fetchClusters, fetchNamespaces } from "@/app/lib/api";
import Item from "./sidebarItem";
import { ClusterCreation, NamespaceCreation } from "./formCreation";
import Link from "next/link";
import { useState, useEffect } from "react";

export function NamespaceSidebar() {
    const [namespaces, setNamespaces] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNamespaces = await fetchNamespaces();
                setNamespaces(fetchedNamespaces);
            } catch (err) {
                setError("Failed to fetch namespaces");
            }
        };
        fetchData();
    }, []);

    return (
        <div className="w-60 h-full flex">
            <List className="w-full overflow-y-auto">
                <div className="mt-2 mb-4 text-center">
                    <NamespaceCreation position="sidebar" />
                </div>
                {error && <p style={{ color: "red" }}>{error}</p>}
                {namespaces.map((namespace) => (
                    <div key={namespace}>
                        <Divider variant="fullWidth" />
                        <Link href={`/namespaces/${namespace}`} passHref>
                            <Item type="namespace" item={namespace} />
                        </Link>
                    </div>
                ))}
                <Divider variant="fullWidth" />
            </List>
            <Divider orientation="vertical" variant="fullWidth" flexItem />
        </div>
    );
}

export function ClusterSidebar({ namespace }: { namespace: string }) {
    const [clusters, setClusters] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedClusters = await fetchClusters(namespace);
                setClusters(fetchedClusters);
            } catch (err) {
                setError("Failed to fetch clusters");
            }
        };
        fetchData();
    }, [namespace]);

    return (
        <div className="w-60 h-full flex">
            <List className="w-full overflow-y-auto">
                <div className="mt-2 mb-4 text-center">
                    <ClusterCreation namespace={namespace} position="sidebar" />
                </div>
                {error && <p style={{ color: "red" }}>{error}</p>}
                {clusters.map((cluster) => (
                    <div key={cluster}>
                        <Divider variant="fullWidth" />
                        <Link href={`/namespaces/${namespace}/clusters/${cluster}`} passHref>
                            <Item type="cluster" item={cluster} namespace={namespace}/>
                        </Link>
                    </div>
                ))}
                <Divider variant="fullWidth" />
            </List>
            <Divider orientation="vertical" variant="fullWidth" flexItem />
        </div>
    );
}
