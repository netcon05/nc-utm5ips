#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations
from nicegui import ui
from helpers import get_names, get_types, get_free_ip


APP_NAME: str = "UTM5 free IPs"


@ui.page("/")
def main():
    with ui.dialog() as dialog, ui.card() as card:
        card.classes("w-auto relative pt-16")
        with ui.row() as titlebar:
            titlebar.classes(
                """
                    absolute left-0 top-0 w-full pl-4 pr-2 py-2
                    text-base bg-primary text-white justify-center
                """
            )
            ui.label(APP_NAME)
        server = ui.select(
            get_names(),
            label="Сервер:",
            on_change=lambda: (
                type.set_options(
                    get_types(server.value) if server.value else []
                ),
                type.set_value(
                    type.options[0] if len(type.options) > 0 else ""
                ),
                address.set_value(
                    get_free_ip(server.value, type.value) if len(type.options) > 0 else ""
                ),
            ),
        ).classes("w-full")
        type = ui.select(
            [],
            label="Тип адреса:",
            on_change=lambda: address.set_value(
                get_free_ip(server.value, type.value) if len(type.options) > 0 else ""
            ),
        ).classes("w-full")
        address = ui.input(label="Адрес:").classes("w-full")
        with ui.row():
            ui.button(
                "Copy", on_click=lambda: ui.clipboard.write(address.value)
            ).classes("w-32")
            ui.button(
                "Update",
                on_click=lambda: address.set_value(
                    get_free_ip(server.value, type.value) if len(type.options) > 0 else ""
                ),
            ).classes("w-32")
        if len(server.options) > 0:
            server.set_value(server.options[0])
    dialog.open()


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title=APP_NAME)
