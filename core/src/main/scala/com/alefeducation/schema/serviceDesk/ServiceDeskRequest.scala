package com.alefeducation.schema.serviceDesk

case class AccessToken(access_token: String)

case class ListInfo (
                      has_more_rows: Boolean,
                      start_index: Int,
                      row_count: Int
                    )

case class Requests (
                      id: String
                    )

case class ServiceDeskDataIds (
                             list_info: ListInfo,
                             requests: Seq[Requests]
                           )

case class RequestBody (
                       list_info: RequestListInfo
                     )

case class RequestListInfo (
                      start_index: Int,
                      sort_field: String,
                      sort_order: String,
                      row_count: Int,
                      search_criteria: Seq[SearchCriteria]
                    )


case class SearchCriteria (
                            field: String,
                            condition: String,
                            values: Option[Seq[String]],
                            logical_operator: String
                          )
