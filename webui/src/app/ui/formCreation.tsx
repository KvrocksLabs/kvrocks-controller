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
import { createNamespaceAction, createClusterAction } from "@/app/lib/actions";
import React from "react";
import FormDialog from "./formDialog";

type NamespaceFormProps = {
  position: string;
};

type ClusterFormProps = {
  position: string;
  namespace: string;
};

export const NamespaceCreation: React.FC<NamespaceFormProps> = ({
    position,
}) => {
    const handleSubmit = async (formData: FormData) => {
        const formObj = Object.fromEntries(formData.entries());
        if (typeof formObj["name"] === "string") {
            return await createNamespaceAction(formObj["name"]);
        }
        return "Invalid form data";
    };

    return (
        <FormDialog
            position={position}
            title="Create Namespace"
            submitButtonLabel="Create"
            formFields={[
                { name: "name", label: "Input Name", type: "text", required: true },
            ]}
            onSubmit={handleSubmit}
        />
    );
};

export const ClusterCreation: React.FC<ClusterFormProps> = ({
    position,
    namespace,
}) => {
    const handleSubmit = async (formData: FormData) => {
        const formObj = Object.fromEntries(formData.entries());
        if (typeof formObj["name"] === "string") {
            const nodes = JSON.parse(String(formObj["nodes"]));
            const replicas = parseInt(String(formObj["replicas"]));
            const password = String(formObj["password"]);

            return await createClusterAction(
                formObj["name"],
                nodes,
                replicas,
                password,
                namespace
            );
        }
        return "Invalid form data";
    };

    return (
        <FormDialog
            position={position}
            title="Create Cluster"
            submitButtonLabel="Create"
            formFields={[
                { name: "name", label: "Input Name", type: "text", required: true },
                { name: "nodes", label: "Input Nodes", type: "array", required: true },
                {
                    name: "replicas",
                    label: "Input Replicas",
                    type: "text",
                    required: true,
                },
                {
                    name: "password",
                    label: "Input Password",
                    type: "text",
                    required: true,
                },
            ]}
            onSubmit={handleSubmit}
        />
    );
};

