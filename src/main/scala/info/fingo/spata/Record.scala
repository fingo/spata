/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import info.fingo.spata.converter.FromProduct
import info.fingo.spata.converter.FromTuple
import info.fingo.spata.converter.ToProduct
import info.fingo.spata.converter.ToTuple
import info.fingo.spata.error.*
import info.fingo.spata.text.FormattedStringParser
import info.fingo.spata.text.ParseResult
import info.fingo.spata.text.StringParser
import info.fingo.spata.text.StringRenderer

import java.util.NoSuchElementException

/** CSV record representation.
  * A record is basically a map from string to string.
  * Values are indexed by header provided explicitly or read from header row in source data.
  * If no header is provided nor can be scanned from source, a tuple-style header `"_1"`, `"_2"` etc. is generated.
  *
  * Position information is always available for parsed data - for records created through [[CSVParser]].
  * It is missing for records created explicitly in application code (in order to be rendered to CSV).
  *
  * @param values core record data
  * @param position record position in source data
  * @param hdr indexing header (field names)
  */
final class Record private (val values: IndexedSeq[String], val position: Option[Position])(hdr: Option[Header]):
  self =>

  /** Safely gets typed record value.
    *
    * Parsers for basic types are provided through [[text.StringParser$ StringParser]] object.
    *
    * To parse optional values provide `Option[?]` as type parameter.
    * Parsing empty value to simple type will result in an error.
    *
    * If wrong header key is provided this function will return `Left[error.HeaderError,?]`.
    * If parsing fails `Left[error.DataError,?]` will be returned.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    * @note When relying on default string parsers, this function assumes "standard" string formatting,
    * without any locale support, e.g. point as decimal separator or ISO date and time formats.
    * Use [[get[A]:* get]] or provide own parser if more control over source format is required.
    * @tparam A type to parse the field to
    * @param key the key of retrieved field
    * @return either parsed value or an error
    */
  def get[A: StringParser](key: String): Decoded[A] = retrieve(key)

  /** Safely gets typed record value.
    *
    * @see [[get]] by key for details.
    *
    * @tparam A type to parse the field to
    * @param idx index of retrieved field
    * @return either parsed value or an error
    */
  def get[A: StringParser](idx: Int): Decoded[A] = retrieve(idx)

  /** Safely gets typed record value.
    *
    * The combination of `get`, [[Field]] constructor and `apply` method allows value retrieval in following form:
    * ```
    * val date: Decoded[LocalDate] = record.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy"))
    * ```
    * (type of formatter is inferred based on target type).
    *
    * Parsers for basic types are available through [[text.StringParser$ StringParser]] object.
    * Additional ones may be provided as given instances.
    *
    * To parse optional values provide `Option[?]` as type parameter.
    * Parsing empty value to simple type will result in an error.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    * @tparam A type to parse the field to
    * @return intermediary to retrieve value according to custom format
    */
  def get[A]: Field[A] = new Field[A]

  /** Converts this record to [[scala.Product]], e.g. case class.
    *
    * For example:
    * ```
    * // Assume following CSV source
    * // ----------------
    * // name,born,died
    * // Nicolaus Copernicus,1473-02-19,1543-05-24
    * // Johannes Hevelius,1611-01-28,
    * // ----------------
    * // and a Record created based on it
    * val record: Record = ???
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * val person: Decoded[Person] = record.to[Person]
    * ```
    *
    * Please note, that the conversion for case classes is name-based
    * (case class field names have to match record (CSV) header) and is case sensitive.
    * The order of fields does not matter.
    * Case class may be narrower and effectively retrieve only a subset of record's fields.
    *
    * It is possible to use a tuple instead of case class - see the tuple-optimized version of [[to]].
    *
    * Current implementation supports only shallow conversion -
    * each product field has to be retrieved from single record field through [[text.StringParser StringParser]].
    *
    * Because conversion to product requires parsing of all fields through [[text.StringParser StringParser]],
    * there is no way to provide custom formatter, like while using [[get[A]:* get]] method.
    * If other then the default formatting has to be handled,
    * a custom given instance `stringParser` has to be provided:
    * ```
    * // Assume following CSV source
    * // ----------------
    * // name,born,died
    * // Nicolaus Copernicus,19.02.1473,24.05.1543
    * // Johannes Hevelius,28.01.1611,
    * // ----------------
    * // and a Record created based on it
    * val record: Record = ???
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * given ldsp: StringParser[LocalDate] with
    *   def apply(str: String) = LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))
    * val person: Decoded[Person] = record.to[Person]
    * ```
    *
    * @tparam P the [[scala.Product]] type to converter this record to,
    * with given type class providing support for conversion (arranged internally by spata,
    * assuming `StringParser` is available for all product field types)
    * @return either converted product or an error
    */
  def to[P <: Product: ToProduct]: Decoded[P] = summon[ToProduct[P]].decode(this)

  /** Converts this record to [[scala.Tuple]].
    * Althogh the product conversion [[to]] method works for tuples too,
    * this tuple-optimized version is more efficient.
    *
    * For example:
    * ```
    * // Assume following CSV source
    * // ----------------
    * // name,born,died
    * // Nicolaus Copernicus,1473-02-19,1543-05-24
    * // Johannes Hevelius,1611-01-28,
    * // ----------------
    * // and a Record created based on it
    * val record: Record = ???
    * tyoe T3 = (String, LocalDate, Option[LocalDate])
    * val person: Decoded[T3] = record.to[T3]
    * ```
    *
    * Please note, that the conversion to tuples is index-based and header names are not taken into account.
    * Tuple may be narrower and effectively retrieve only an initial subset of record's fields.
    *
    * Current implementation supports only shallow conversion -
    * each tuple field has to be retrieved from single record field through [[text.StringParser StringParser]].
    *
    * Because conversion to tuple requires parsing of all fields through [[text.StringParser StringParser]],
    * there is no way to provide custom formatter, like while using [[get[A]:* get]] method.
    * If other then the default formatting has to be handled,
    * a custom given instnce of `stringParser` has to be provided:
    * ```
    * // Assume following CSV source
    * // ----------------
    * // name,born,died
    * // Nicolaus Copernicus,19.02.1473,24.05.1543
    * // Johannes Hevelius,28.01.1611,
    * // ----------------
    * // and a Record created based on it
    * val record: Record = ???
    * tyoe T3 = (String, LocalDate, Option[LocalDate])
    * given ldsp: StringParser[LocalDate] with
    *   def apply(str: String) = LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))
    * val person: Decoded[T3] = record.to[T3]
    * ```
    *
    * @tparam T the [[scala.Tuple]] type to converter this record to,
    * with given type class providing support for conversion (arranged internally by spata,
    * assuming `StringParser` is available for all tuple field types)
    * @return either converted tuple or an error
    */
  def to[T <: Tuple: ToTuple]: Decoded[T] = summon[ToTuple[T]].decode(this)

  /** Gets field value.
    *
    * @param key the key of retrieved field
    * @return field value in its original, string format if correct key is provided or `None` otherwise.
    */
  def apply(key: String): Option[String] =
    header(key).flatMap(apply)

  /** Gets field value.
    *
    * @param idx the index of retrieved field, starting from `0`.
    * @return field value in original, string format or `None` if index is out of bounds.
    */
  def apply(idx: Int): Option[String] = values.unapply(idx)

  /** Gets number of fields in record. */
  def size: Int = values.size

  /** Creates new record with value at given key updated with provided one.
    * Original record is returned if the key does not match any header key.
    *
    * The new record shares the header with the old one.
    *
    * @param key the key (name) of field to be updated
    * @param value the new value
    * @return a new record with updated value
    */
  def updated(key: String, value: String): Record = updatedWith(key)(_ => value)

  /** Creates new record with value at given key updated by provided function.
    * The function receives existing value as an argument.
    * Original record is returned if the key does not match any header key.
    *
    * The new record shares the header with the old one.
    *
    * @example
    * ```
    * val updated = record.updatedWith("name")(s => s.toLowerCase)
    * ```
    *
    * @param key the key (name) of field to be updated
    * @param f the function to be applied to existing value
    * @return a new record with updated value
    */
  def updatedWith(key: String)(f: String => String): Record =
    val idx = header(key)
    idx.map(updatedWith(_)(f)).getOrElse(this)

  /** Creates new record with value at given index updated with provided one.
    * Original record is returned if the index is out of bounds.
    *
    * The new record shares the header with the old one.
    *
    * @param idx the index of field to be updated
    * @param value the new value
    * @return a new record with updated value
    */
  def updated(idx: Int, value: String): Record = updatedWith(idx)(_ => value)

  /** Creates new record with value at given index updated by provided function.
    * The function receives existing value as an argument.
    * Original record is returned if the index is out of bounds.
    *
    * The new record shares the header with the old one.
    *
    * @param idx the index of field to be updated
    * @param f the function to be applied to existing value
    * @return a new record with updated value
    */
  def updatedWith(idx: Int)(f: String => String): Record =
    this(idx)
      .map: v =>
        val nvs = values.updated(idx, f(v))
        hdr match // do not access and create header if not necessary
          case Some(h) => Record(nvs*)(h)
          case None => Record.fromValues(nvs*)
      .getOrElse(this)

  /** Creates new record with value at given key updated by provided function.
    * The function receives existing, typed value as an argument.
    *
    * This method may fail if incorrect key is provided or existing value cannot be parsed to requested type.
    *
    * The new record shares the header with the old one.
    *
    * @example
    * ```
    * val kmToMile = record.altered[Double,Double]("distance")(km => 0.621371 * km)
    * ```
    *
    * @param key the key (name) of field to be updated
    * @param f the function to be applied to existing value
    * @tparam A type of existing value
    * @tparam B type of new value
    * @return a new record with updated value or an error
    */
  def altered[A: StringParser, B: StringRenderer](key: String)(f: A => B): Either[ContentError, Record] =
    get[A](key).map: v =>
      val nv = StringRenderer.render[B](f(v))
      updated(key, nv)

  /** Creates a builder, initialized with content of this record.
    * A builder may be used to enhance or reduce record.
    *
    * Please note, that this method creates new header for each patched record.
    *
    * @example
    * ```
    * val imperial = record.patch.remove("m").add("foot", foot).add("yard", yard).get
    * ```
    *
    * @see [[Record.Builder]]
    * @return record builder
    */
  def patch: Record.Builder = new Record.Builder(header.names.zip(values).toList)

  /** Indexing header - provided explicitly or generated in tuple style: `"_1"`, `"_2"` etc. */
  lazy val header: Header = hdr.getOrElse(Header(size)) // generate header only if needed

  /** Row number in source data this record comes from or `0` if this information is not available
    * (i.e. record is not created through CSV parsing).
    *
    * @see [[Position]] for row number description.
    */
  lazy val rowNum: Int = position.map(_.row).getOrElse(0)

  /** Last line number in source data this record is built from or `0` if this information is not available
    * (i.e. record is not created through CSV parsing).
    *
    * @see [[Position]] for line number description.
    */
  lazy val lineNum: Int = position.map(_.line).getOrElse(0)

  /** Gets text representation of record, with fields separated by comma. */
  override def toString: String = values.mkString(",")

  /* Retrieve field value by key using string parser with provided format */
  private def retrieve[A, B](key: String, fmt: B)(using parser: FormattedStringParser[A, B]): Decoded[A] =
    decode(key)(StringParser.parse(_, fmt))

  /* Retrieve field value by key using string parser */
  private def retrieve[A](key: String)(using parser: StringParser[A]): Decoded[A] =
    decode(key)(StringParser.parse)

  /* Retrieve field value by index using string parser with provided format */
  private def retrieve[A, B](idx: Int, fmt: B)(using parser: FormattedStringParser[A, B]): Decoded[A] =
    decode(idx)(StringParser.parse(_, fmt))

  /* Retrieve field value by index using string parser */
  private def retrieve[A](idx: Int)(using parser: StringParser[A]): Decoded[A] =
    decode(idx)(StringParser.parse)

  /* Decode field value using provided string parsing function. Wraps error into proper CSVException subclass. */
  private def decode[A](key: String)(parse: String => ParseResult[A]): Decoded[A] =
    val value = apply(key)
    value match
      case Some(str) =>
        parse(str) match
          case Right(value) => Right(value)
          case Left(error) => Left(new DataError(error.content, position, FieldInfo(key), error))
      case None => Left(new HeaderError(position, FieldInfo(key)))

  /* Decode field value using provided string parsing function. Wraps error into proper CSVException subclass. */
  private def decode[A](idx: Int)(parse: String => ParseResult[A]): Decoded[A] =
    val value = apply(idx)
    value match
      case Some(str) =>
        parse(str) match
          case Right(value) => Right(value)
          case Left(error) => Left(new DataError(error.content, position, FieldInfo(idx), error))
      case None => Left(new IndexError(position, FieldInfo(idx)))

  /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
    * Provides exception-free parsing method.
    *
    * @tparam A target type for parsing
    */
  final class Field[A]:

    /** Safely parses string to desired type based on provided format.
      *
      * If wrong header key is provided this function will return `Left[error.HeaderError,?]`.
      * If parsing fails `Left[error.DataError,?]` will be returned.
      *
      * @param key the name of retrieved field
      * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of formatter
      * @return either parsed value or an error
      */
    def apply[B](key: String, fmt: B)(using parser: FormattedStringParser[A, B]): Decoded[A] = retrieve(key, fmt)

    /** Safely parses string to desired type based on provided format.
      *
      * If wrong index is provided this function will return `Left[error.IndexError,?]`.
      * If parsing fails `Left[error.DataError,?]` will be returned.
      *
      * @param idx the index of retrieved field
      * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of formatter
      * @return either parsed value or an error
      */
    def apply[B](idx: Int, fmt: B)(using parser: FormattedStringParser[A, B]): Decoded[A] = retrieve(idx, fmt)

  /** Access to unsafe (exception throwing) methods */
  object unsafe:

    /** Gets typed record value.
      *
      * Parsers for basic types are provided through [[text.StringParser$ StringParser]] object.
      *
      * To parse optional values provide `Option[?]` as type parameter.
      * Parsing empty value to simple type will throw an exception.
      *
      * @see [[text.StringParser StringParser]] for information on providing custom parsers.
      * @note When relying on default string parsers, this function assumes "standard" string formatting,
      *       without any locale support, e.g. point as decimal separator or ISO date and time formats.
      *       Use [[get[A]:* get]] or provide own parser if more control over source format is required.
      * @tparam A type to parse the field to
      * @param key the key of retrieved field
      * @return parsed value
      * @throws error.ContentError if field cannot be parsed to requested type or incorrect `key` is provided
      */
    @throws[ContentError]("if field cannot be parsed to requested type or wrong key is provided")
    def get[A: StringParser](key: String): A = rethrow(retrieve(key))

    /** Gets typed record value. Supports custom string formats through [[text.StringParser.Pattern Pattern]].
      *
      * The combination of `get`, [[Field]] constructor and `apply` method
      * allows value retrieval in following form:
      * ```
      * val date: LocalDate = record.unsafe.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy"))
      * ```
      * (type of formatter is inferred based on target type).
      *
      * Parsers for basic types (as required by [[Field]])
      * are available through [[text.StringParser$ StringParser]] object.
      * Additional ones may be provided as given instances.
      *
      * To parse optional values provide `Option[?]` as type parameter.
      * Parsing empty value to simple type will throw an exception.
      *
      * @see [[text.StringParser StringParser]] for information on providing custom parsers.
      * @tparam A type to parse the field to
      * @return intermediary to retrieve value according to custom format
      */
    def get[A]: Field[A] = Field[A]

    /** Gets field value.
      *
      * @param key the key of retrieved field
      * @return field value in original, string format
      * @throws NoSuchElementException when incorrect `key` is provided
      */
    @throws[NoSuchElementException]("when incorrect key is provided")
    def apply(key: String): String = self(key).get

    /** Gets field value.
      *
      * @param idx the index of retrieved field, starting from `0`
      * @return field value in original, string format
      * @throws IndexOutOfBoundsException when incorrect index is provided
      */
    @throws[IndexOutOfBoundsException]("when incorrect index is provided")
    def apply(idx: Int): String = values(idx)

    /* Throws exception if Either is Left. */
    private def rethrow[A](result: Decoded[A]) = result match
      case Right(value) => value
      case Left(error) => throw error

    /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
      *
      * @tparam A target type for parsing
      */
    final class Field[A]:

      /** Parses field to desired type based on provided format.
        *
        * @param key the name of retrieved field
        * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
        * @param parser the parser for specific target type
        * @tparam B type of concrete formatter
        * @return parsed value
        * @throws error.ContentError if field cannot be parsed to requested type or incorrect `key` is provided
        */
      @throws[ContentError]("if field cannot be parsed to requested type or incorrect key is provided")
      def apply[B](key: String, fmt: B)(using parser: FormattedStringParser[A, B]): A = rethrow(retrieve(key, fmt))

      /** Parses field to desired type based on provided format.
        *
        * @param idx the index of retrieved field
        * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
        * @param parser the parser for specific target type
        * @tparam B type of concrete formatter
        * @return parsed value
        * @throws error.ContentError if field cannot be parsed to requested type or incorrect `idx` is provided
        */
      @throws[ContentError]("if field cannot be parsed to requested type or incorrect index is provided")
      def apply[B](idx: Int, fmt: B)(using parser: FormattedStringParser[A, B]): A = rethrow(retrieve(idx, fmt))

/** Record helper object. Used to create and convert records. */
object Record:

  /* Creates `Record`. See Record class for more information about parameters.
   * This method is used by CSVParser to validate header and content conformance of parsed records.
   */
  private[spata] def create(values: IndexedSeq[String], rowNum: Int, lineNum: Int)(
    header: Header
  ): Either[StructureException, Record] =
    if values.size == header.size
    then Right(new Record(values, Position.some(rowNum, lineNum))(Some(header)))
    else Left(new StructureException(ParsingErrorCode.WrongNumberOfFields, Position.some(rowNum, lineNum)))

  /** Creates record.
    *
    * The size of header has to match the number of values.
    * In the rare case when it does not, the header is shrunk or extended accordingly.
    * While extending, the new values are added in tuple-style, with values matching their position
    * (e.g. extending `Header("X","Y","Z")` by 1 yields `Header("X","Y","Z","_4")`)
    *
    * @param values the values forming the record
    * @param header record header - keys for values (field names)
    * @return new record
    */
  def apply(values: String*)(header: Header): Record =
    val HS = header.size
    val hdr = values.size match
      case HS => header
      case vs if vs < HS => header.shrink(vs)
      case vs => header.extend(vs)
    new Record(values.toIndexedSeq, Position.none)(Some(hdr))

  /** Creates record from list of values.
    * Record header is created in tuple-like form: `_1`, `_2`, `_3` etc.
    * Header creation is lazy - records created in this way may be effectively headerless,
    * if values are accessed by index only.
    *
    * @param values list of values forming record
    * @return new record
    */
  def fromValues(values: String*): Record = new Record(values.toIndexedSeq, Position.none)(None)

  /** Creates record from key-value pairs.
    * Keys are extracted as header.
    *
    * This method does not enforce unique keys (as it is not enforced by header constructor).
    * Providing duplicates does not cause any erroneous conditions while accessing record data,
    * however the values associated with second and next duplicated keys will be accessible only by index.
    *
    * @param keysValues list of keys (names) and values forming record
    * @return new record
    */
  def fromPairs(keysValues: (String, String)*): Record =
    val (k, v) = keysValues.unzip
    new Record(v.toIndexedSeq, Position.none)(Some(Header(k*)))

  /** Creates a record from [[scala.Product]], e.g. case class.
    *
    * This renders product (class) values to string representation. In contrast to simple `toString` operation,
    * this process allows more control over output format,
    * e.g. taking into account locale or render simple value from [[scala.Option]].
    *
    * ```
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * val person: Person = Person("Nicolaus Copernicus", LocalDate.of(1473,2,19), Some(LocalDate.of(1543,5,24)))
    * val record: Record = Record.from(person)
    * ```
    *
    * Please note, that the conversion is name-based (case class field names are used to form record header)
    * and is case sensitive.
    *
    * It is possible to use a tuple instead of case class, using tuple-optimized [[from]] method.
    *
    * Current implementation supports only shallow conversion -
    * each product field has to be rendered to single record field through [[text.StringRenderer StringRenderer]].
    *
    * Because conversion from product requires rendering of all fields through [[text.StringRenderer StringRenderer]],
    * there is no way to provide custom formatter.
    * If other then the default formatting has to be used,
    * a custom given instance of `stringRenderer` has to be provided:
    * ```
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * val person: Person = Person("Nicolaus Copernicus", LocalDate.of(1473,2,19), Some(LocalDate.of(1543,5,24)))
    * given ldsr: StringRenderer[LocalDate] with
    *   def apply(date: LocalDate) = DateTimeFormatter.ofPattern("dd.MM.yyyy").format(date)
    * val record: Record = Record.from(person)
    * ```
    *
    * @param product the product to convert (render) to record
    * @tparam P the [[scala.Product]] type to create new record from
    * with given type class providing support for conversion (arranged internally by spata,
    * assuming `StringRenderer` is available for all product field types)
    * @return new record
    */
  def from[P <: Product: FromProduct](product: P): Record = summon[FromProduct[P]].encode(product)

  /** Creates a record from [[scala.Tuple]].
    * Althogh conversion [[from]] product works for tuples too, this tuple-optimized version is more efficient.
    *
    * This renders tuple values to string representation. In contrast to simple `toString` operation,
    * this process allows more control over output format,
    * e.g. taking into account locale or render simple value from [[scala.Option]].
    *
    * ```
    * type T3 = (String, LocalDate, Option[LocalDate])
    * val tuple: T3 = ("Nicolaus Copernicus", LocalDate.of(1473,2,19), Some(LocalDate.of(1543,5,24)))
    * val record: Record = Record.from(tuple)
    * ```
    *
    * Please note, that the conversion is index-based
    * and the header will be created from tuple field naming convention: `_1`, `_2` etc.
    *
    * Current implementation supports only shallow conversion -
    * each tuple field has to be rendered to single record field through [[text.StringRenderer StringRenderer]].
    *
    * Because conversion from tuple requires rendering of all fields through [[text.StringRenderer StringRenderer]],
    * there is no way to provide custom formatter.
    * If other then the default formatting has to be used,
    * a custom given instance of `stringRenderer` has to be provided:
    * ```
    * type T3 = (String, LocalDate, Option[LocalDate])
    * val tuple: T3 = ("Nicolaus Copernicus", LocalDate.of(1473,2,19), Some(LocalDate.of(1543,5,24)))
    * given ldsr: StringRenderer[LocalDate] with
    *   def apply(date: LocalDate) = DateTimeFormatter.ofPattern("dd.MM.yyyy").format(date)
    * val record: Record = Record.from(tuple)
    * ```
    *
    * @param tuple the tuple to convert (render) to record
    * @tparam T the [[scala.Tuple]] type to create new record from
    * with given type class providing support for conversion (arranged internally by spata,
    * assuming `StringRenderer` is available for all tuple field types)
    * @return new record
    */
  def from[T <: Tuple: FromTuple](tuple: T): Record = summon[FromTuple[T]].encode(tuple)

  /** Creates new record builder. */
  def builder: Builder = Builder(List.empty[(String, String)])

  /** Extension of [[scala.Product]] to provide convenient conversion to records. */
  trait ProductOps:
    /** Converts [[scala.Product]] (e.g. case class) to [[Record]].
      *
      * @see [[Record.from]] for more information.
      *
      * @param product product to extend
      * @tparam P concrete type of product with given type class providing support for conversion
      * @return new record
      */
    extension [P <: Product: FromProduct](product: P) def toRecord: Record = summon[FromProduct[P]].encode(product)

    /** Converts [[scala.Tuple]] to [[Record]].
      * Althogh product conversion works for tuples too, this tuple-optimized version is more efficient.
      *
      * @see [[Record.from]] for more information.
      *
      * @param tuple tuple to extend
      * @tparam T concrete type of tuple with given type class providing support for conversion
      * @return new record
      */
    extension [T <: Tuple: FromTuple](tuple: T) def toRecord: Record = summon[FromTuple[T]].encode(tuple)

  /** Given instance to easily bring ProductOps extension in scope. */
  given ProductOps: ProductOps()

  /** Helper to incrementally build records from typed values.
    * Supports also values removal.
    *
    * @param buf buffer used to incrementally build record's content.
    * @param removed keys of fields to be removed from record
    */
  final class Builder private (buf: List[(String, String)], removed: Set[String]):

    /* Auxiliary constructor, to simplify builder creation */
    private[Record] def this(buf: List[(String, String)]) = this(buf, Set.empty[String])

    /** Enhances builder with a new value.
      *
      * Adding value with already existing key results in header duplication.
      * Although this is correctly handled, duplicated header causes some value inaccessible by their name.
      *
      * @param key the key (field name) of added value
      * @param value the added value
      * @param renderer the renderer to convert the value to string
      * @tparam A value type
      * @return builder augmented with the new value
      */
    def add[A](key: String, value: A)(using renderer: StringRenderer[A]): Builder =
      Builder((key, renderer(value)) :: buf, removed)

    /** Reduces builder removing given value from it.
      *
      * If the key is not found, the method does nothing.
      * If there are multiple fields with the same name (header key), all instances are removed.
      *
      * Actual values removal is done while constructing the final record.
      * As a result, even if the call to `remove` precedes the call to `add`, the added field will be removed:
      * ```
      * val record = Record.builder.remove("field").add("field", 100) // gets empty record
      * ```
      *
      * @param key the key (field name) of removed value
      * @return builder stripped of selected value
      */
    def remove(key: String): Builder = Builder(buf, removed + key)

    /** Gets final record from this builder.
      *
      * @return new record with values from this builder.
      */
    def get: Record =
      val result = if removed.isEmpty then buf else buf.filterNot((k, _) => removed.contains(k))
      Record.fromPairs(result.reverse*)

    /* Gets final record from this builder with reversed order of the fields,
     * which really means preserving the order, because values ate prepended.
     */
    private[spata] def reversed: Record = Record.fromPairs(buf*)

  /** Given instance to convert record builder to record. */
  given buildRecord: Conversion[Builder, Record] = _.get
