#' Retrieve Data from MongoDB
#'
#' This function connects to a MongoDB database and retrieves all documents from a specified collection.
#'
#' @param connection_string A character string specifying the MongoDB connection URL. Default is NULL.
#' @param collection_name A character string specifying the name of the collection to query. Default is NULL.
#' @param db_name A character string specifying the name of the database. Default is NULL.
#'
#' @return A data frame containing all documents from the specified collection.
#'
#' @importFrom mongolite mongo
#'
#' @examples
#' \dontrun{
#' # Retrieve data from a MongoDB collection
#' result <- mongodb_get(
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection <- function(connection_string = NULL, collection_name = NULL, db_name = NULL){
  data <- mongolite::mongo(collection=collection_name, db=db_name, url=connection_string)
  data$find()
}

