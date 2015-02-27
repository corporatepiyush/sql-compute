###
    @author Piyush Katariya
###


# Exceptions

class DataFormatError extends Error

  constructor : (clause, requiredFormat) ->
    @name = 'DataFormatError'
    @message = "The value of '#{clause}' should be of type #{requiredFormat}"


class DependancyError extends Error

  constructor : (clause, requiredClause) ->
    @name = 'DependancyError'
    @message = "'#{requiredClause}' is required if '#{clause}' is used"



# Data Source

class DataSource


class ArrayDataSource extends DataSource

  constructor : (@array) ->


class CustomDataSource extends DataSource

  constructor : (@promise) ->




# Query Engine

class Operation

  constructor : (@command) ->


class Select extends Operation

  validate : ->
    unless @command.select instanceof Array
      throw new DataFormatError('select', 'Array')


class From extends Operation

  validate : ->
    unless @command.from instanceof Array
      throw new DataFormatError('from', 'Array')

    for db in @command.from
      for name, source of db
        unless source instanceof DataSource
          throw new DataFormatError("datasource #{name}", 'either ArrayDataSource or CustomDataSource')


class Where extends Operation

  validate : ->
    unless @command.where instanceof Function
      throw new DataFormatError('where', 'Function')


class GroupBy extends Operation

  validate : ->
    unless @command.select instanceof Array
      throw new DataFormatError('groupBy', 'Array')


class Having extends Operation

  validate : ->
    unless @command.groupBy
      throw new DependancyError('having', 'groupBy')
    unless @command.having.condition
      throw new DependancyError('having', 'having.condition')
    unless @command.having.condition instanceof Function
      throw new DataFormatError('having.condition', 'Function')

class OrderBy extends Operation

  validate : ->
    unless @command.orderBy instanceof Array
      throw new DataFormatError('orderBy', 'Array')


class Limit extends Operation

  validate : ->
    unless typeof @command.limit is 'number'
      throw new DataFormatError('limit', 'Number')


class Offset extends Operation

  validate : ->
    unless typeof @command.offset is 'number'
      throw new DataFormatError('offset', 'Number')


class Query

  constructor : (@command, ops) ->
    @pipeline = []
    @pipeline.push new ops.Where(@command) if @command.where
    @pipeline.push new ops.GroupBy(@command) if @command.groupBy
    @pipeline.push new ops.Having(@command) if @command.groupBy
    @pipeline.push new ops.Select(@command)
    @pipeline.push new ops.OrderBy(@command) if @command.orderBy
    @pipeline.push new ops.Offset(@command) if @command.offset
    @pipeline.push new ops.Limit(@command) if @command.limit

  validate : ->
    unless typeof @command is 'object'
      throw new DataFormatError('Query Object', 'JSON')
    unless @command.select and @command.from
      throw new DependancyError('Query Object', '"select" and "from"')
    for operation in @pipeline
      operation.validate()


  execute : (success, error) ->
    try
      result = new ops.From(@command) #async
      result.success = (data) ->
        try
          for operation in @pipeline  #sync
            data = operation.execute(data)
          success(data)
        catch err
          error(err)
      result.error = (err) -> error(err)
      result.execute()
    catch err
      error(err)


# Global exports

window.sqlCompute = window.sqlCompute ||
  Query : Query
  Select : Select
  From : From
  Where : Where
  GroupBy : GroupBy
  Having : Having
  OrderBy : OrderBy
  Limit : Limit
  Offset : Offset
  ArrayDataSource : (arr) -> new ArrayDataSource arr
  CustomDataSource : (fn) -> new CustomDataSource fn()
