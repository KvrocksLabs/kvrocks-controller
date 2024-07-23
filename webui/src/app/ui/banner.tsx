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

import { AppBar, Container, Toolbar } from "@mui/material";
import Image from "next/image";
import NavLinks from "./nav-links";

const links = [
    {
        url: '/',
        title: 'Home'
    },{
        url: '/namespaces',
        title: 'Namespaces'
    },{
        url: 'https://kvrocks.apache.org',
        title: 'community',
        _blank: true
    },
]

export default function Banner() {
    return (<AppBar>
        <Container maxWidth={false}>
            <Toolbar className="space-x-4">
                <Image src="/logo.svg" width={40} height={40} alt='logo'></Image>
                <NavLinks links={links}/>
            </Toolbar>
        </Container>
    </AppBar>)
}