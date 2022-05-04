/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */
package queryExecutor

import collection.mutable.HashMap

/**
 * Simple class for query description
 */
class Query(qN: String, tN:String, sT:String, sgSQ:String, igSQ:String) {

  var queryName = qN
  // SQL query type as string
  var tNameString: String = tN
  // SQL query as string
  var sTripleString: String = sT
  // Table load instruction as string
  var sgSQString: String = sgSQ
  // The list of tables, which exist in query
  var igSQString:String = igSQ
  
}
