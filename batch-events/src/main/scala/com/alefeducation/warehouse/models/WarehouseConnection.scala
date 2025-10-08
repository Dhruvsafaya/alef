package com.alefeducation.warehouse.models

final case class WarehouseConnection(schema: String, connectionUrl: String, driver: String, username: String, password: String)