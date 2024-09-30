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

import { Box, Container, Card, Link, Typography } from "@mui/material";
import { NamespaceSidebar } from "../ui/sidebar";
import { useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { fetchNamespaces } from "../lib/api";
import { LoadingSpinner } from "../ui/loadingSpinner";
import { CreateCard } from "../ui/createCard";

export default function Namespace() {
    const [namespaces, setNamespaces] = useState<string[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNamespaces = await fetchNamespaces();

                setNamespaces(fetchedNamespaces);
            } catch (error) {
                console.error("Error fetching namespaces:", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    return (
        <div className="flex h-full">
            <NamespaceSidebar />
            <Container
                maxWidth={false}
                disableGutters
                sx={{ height: "100%", overflowY: "auto", marginLeft: "16px" }}
            >
                <div className="flex flex-row flex-wrap">
                    {namespaces.length !== 0 ? (
                        namespaces.map(
                            (namespace, index) =>
                                namespace && (
                                    <Link key={namespace} href={`/namespaces/${namespace}`}>
                                        <CreateCard>
                                            <Typography variant="h6">
                                                {namespace} Namespace
                                            </Typography>
                                        </CreateCard>
                                    </Link>
                                )
                        )
                    ) : (
                        <Box>
                            <Typography variant="h6">
                No namespaces found, create one to get started
                            </Typography>
                        </Box>
                    )}
                </div>
            </Container>
        </div>
    );
}
