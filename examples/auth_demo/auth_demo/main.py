# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Implement global logic for running the application."""

import asyncio

import uvicorn
from auth_demo.config import Config
from auth_demo.container import Container  # type: ignore
from auth_demo.router import router
from fastapi import FastAPI
from httpyexpect.server.handlers.fastapi_ import configure_exception_handler


def get_configured_container(config: Config) -> Container:
    """Create and configure the DI container."""
    container = Container()
    container.config.load_config(config)
    return container


def get_app() -> FastAPI:
    """Create a FastAPI app."""
    app = FastAPI()
    app.include_router(router)
    configure_exception_handler(app)
    return app


async def run_server():
    """Run the HTTP API."""
    config = Config()
    async with get_configured_container(config) as container:
        container.wire(modules=["auth_demo.router", "auth_demo.auth.connector"])
        uv_config = uvicorn.Config(app=get_app())
        server = uvicorn.Server(uv_config)
        await server.serve()


def run():
    """Main entry point for running the server."""
    asyncio.run(run_server())
