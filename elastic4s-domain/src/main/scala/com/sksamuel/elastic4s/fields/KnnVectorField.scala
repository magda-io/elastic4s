package com.sksamuel.elastic4s.fields

sealed trait SpaceType extends Product with Serializable {
  def name: String
}

object SpaceType {
  val defaultValue: SpaceType = l2

  val values = Set(l1, l2, innerProduct, cosine, linf)

  def withName(name: String): Option[SpaceType] =
    values.map(v => v.name -> v).toMap.get(name)

  // L1 distance
  case object l1 extends SpaceType { val name = "l1" }
  // L2 distance
  case object l2 extends SpaceType { val name = "l2" }
  // Dot product (or inner product) similarity
  case object innerProduct extends SpaceType {
    val name = "innerproduct"
  }
  // Cosine similarity
  case object cosine extends SpaceType { val name = "cosinesimil" }
  // The Linf (for L infinity) distance
  case object linf extends SpaceType { val name = "linf" }
}

sealed trait KnnEngine {
  def name: String
}

object KnnEngine {
  val defaultValue: KnnEngine = nmslib
  val values = Set(nmslib, faiss, lucene)

  def withName(name: String): Option[KnnEngine] =
    values.map(v => v.name -> v).toMap.get(name)

  case object nmslib extends KnnEngine { val name = "nmslib" }
  case object faiss extends KnnEngine { val name = "faiss" }
  case object lucene extends KnnEngine { val name = "lucene" }
}

sealed trait KnnMethod {
  def name: String
}

object KnnMethod {
  val defaultValue = hnsw
  val values = Set(hnsw, ivf)

  def withName(name: String): Option[KnnMethod] =
    values.map(v => v.name -> v).toMap.get(name)

  case object hnsw extends KnnMethod { val name = "hnsw" }
  case object ivf extends KnnMethod { val name = "ivf" }
}

sealed trait KnnMethodEncoderName {
  def name: String
}

sealed trait FaissEncoderName extends KnnMethodEncoderName

object FaissEncoderName {
  val defaultValue = flat
  val values = Set(flat, pq, sq)

  def withName(name: String): Option[FaissEncoderName] =
    values.map(v => v.name -> v).toMap.get(name)

  case object flat extends FaissEncoderName { val name = "flat" }
  case object pq extends FaissEncoderName { val name = "pq" }
  case object sq extends FaissEncoderName { val name = "sq" }
}

sealed trait FaissScalarQuantizationType {
  def name: String
}

object FaissScalarQuantizationType {
  val defaultValue = fp16
  val values = Set(fp16)

  def withName(name: String): Option[FaissScalarQuantizationType] =
    values.map(v => v.name -> v).toMap.get(name)

  case object fp16 extends FaissScalarQuantizationType { val name = "fp16" }
}

sealed trait KnnMethodEncoder {
  def name: Option[KnnMethodEncoderName]
}

case class FaissEncoder(
    name: Option[FaissEncoderName] = None,
    m: Option[Int] = None,
    codeSize: Option[Int] = None,
    sqClip: Option[Boolean] = None,
    sqType: Option[FaissScalarQuantizationType] = None
) extends KnnMethodEncoder

object FaissEncoder {

  def apply(
      name: Option[FaissEncoderName] = None,
      m: Option[Int] = None,
      codeSize: Option[Int] = None,
      sqClip: Option[Boolean] = None,
      sqType: Option[FaissScalarQuantizationType] = None
  ): FaissEncoder = {
    name.getOrElse(FaissEncoderName.flat) match {
      case FaissEncoderName.pq =>
        if (sqClip.nonEmpty || sqType.nonEmpty) {
          throw new IllegalArgumentException(
            "sqClip or sqType parameter is not available for pq encoder type"
          )
        }
      case FaissEncoderName.sq =>
        if (m.nonEmpty || codeSize.nonEmpty) {
          throw new IllegalArgumentException(
            "m or codeSize parameter is not available for sq encoder type"
          )
        }
      case _ =>
        if (
          m.nonEmpty || codeSize.nonEmpty || sqClip.nonEmpty || sqType.nonEmpty
        ) {
          throw new IllegalArgumentException(
            "flat encoder type doesn't take any parameters"
          )
        }
    }
    new FaissEncoder(
      name = name,
      m = m,
      codeSize = codeSize,
      sqClip = sqClip,
      sqType = sqType
    )
  }
}

sealed trait KnnMethodParameters {
  def name: String
  def engine: Option[KnnEngine]
  def spaceType: Option[SpaceType]
  def encoder: Option[KnnMethodEncoder]

  private val hmslibSpaceTypes = Set(
    SpaceType.l1,
    SpaceType.l2,
    SpaceType.innerProduct,
    SpaceType.cosine,
    SpaceType.linf
  )
  private val faissSpaceTypesWithPQ: Set[SpaceType] = Set(SpaceType.l2)
  private val faissSpaceTypes = Set(SpaceType.l2, SpaceType.innerProduct)
  private val luceneSpaceTypes = Set(
    SpaceType.l2,
    SpaceType.innerProduct,
    SpaceType.cosine
  )

  def validateSpaceType() = {
    val spaceType = this match {
      case p: HnswParameters => p.spaceType
      case p: IvfParameters  => p.spaceType
    }

    val engine = (this match {
      case p: HnswParameters => p.engine
      case p: IvfParameters  => p.engine
    }).getOrElse(KnnEngine.defaultValue)

    val encoder = this match {
      case p: HnswParameters => p.encoder
      case p: IvfParameters  => p.encoder
    }

    if (spaceType.nonEmpty) {
      val spaceTypeVal = spaceType.get
      engine match {
        case KnnEngine.nmslib =>
          if (!hmslibSpaceTypes.contains(spaceTypeVal)) {
            throw new IllegalArgumentException(
              s"""hmslib engine doesn't support space_type ${spaceTypeVal.name}"""
            )
          }
        case KnnEngine.faiss =>
          val usePQ = encoder.nonEmpty && (encoder.get match {
            case e: FaissEncoder =>
              e.name.nonEmpty && e.name.get == FaissEncoderName.pq
            case _ => false
          })
          if (usePQ) {
            if (!faissSpaceTypesWithPQ.contains(spaceTypeVal)) {
              throw new IllegalArgumentException(
                s"""faiss engine with PQ doesn't support space_type ${spaceTypeVal.name}"""
              )
            }
          } else {
            if (!faissSpaceTypes.contains(spaceTypeVal)) {
              throw new IllegalArgumentException(
                s"""faiss engine doesn't support space_type ${spaceTypeVal.name}"""
              )
            }
          }
        case KnnEngine.lucene =>
          if (!luceneSpaceTypes.contains(spaceTypeVal)) {
            throw new IllegalArgumentException(
              s"""lucene engine doesn't support space_type ${spaceTypeVal.name}"""
            )
          }
      }
    }
  }
}

case class HnswParameters(
    engine: Option[KnnEngine] = None,
    spaceType: Option[SpaceType] = None,
    efConstruction: Option[Int] = None,
    m: Option[Int] = None,
    efSearch: Option[Int] = None,
    encoder: Option[KnnMethodEncoder] = None
) extends KnnMethodParameters {
  val name = "hnsw"
}

object HnswParameters {

  def apply(
      engine: Option[KnnEngine] = None,
      spaceType: Option[SpaceType] = None,
      efConstruction: Option[Int] = None,
      m: Option[Int] = None,
      efSearch: Option[Int] = None,
      encoder: Option[KnnMethodEncoder] = None
  ): HnswParameters = {
    if (engine.nonEmpty) {
      engine.get match {
        case KnnEngine.faiss =>
        // do nothing as all parameters are accepted
        case _ =>
          if (efSearch.nonEmpty || encoder.nonEmpty) {
            throw new IllegalArgumentException(
              "nmslib or lucene doesn't support ef_search or encoder"
            )
          }
      }
    }
    val p = new HnswParameters(
      engine = engine,
      spaceType = spaceType,
      efConstruction = efConstruction,
      m = m,
      efSearch = efSearch,
      encoder = encoder
    )
    p.validateSpaceType()
    p
  }
}

case class IvfParameters(
    engine: Option[KnnEngine] = None,
    spaceType: Option[SpaceType] = None,
    nlist: Option[Int] = None,
    nprobes: Option[Int] = None,
    encoder: Option[KnnMethodEncoder] = None
) extends KnnMethodParameters {
  val name = "ivf"
}

object IvfParameters {
  def apply(
      engine: Option[KnnEngine] = None,
      spaceType: Option[SpaceType] = None,
      nlist: Option[Int] = None,
      nprobes: Option[Int] = None,
      encoder: Option[KnnMethodEncoder] = None
  ): IvfParameters = {
    if (engine.nonEmpty) {
      engine.get match {
        case KnnEngine.faiss =>
          // do nothing as all parameters are accepted
          if (encoder.nonEmpty && !encoder.get.isInstanceOf[FaissEncoder]) {
            throw new IllegalArgumentException(
              "encoder must be instance of FaissEncoder for faiss engine"
            )
          }
        case _ =>
          throw new IllegalArgumentException(
            "only faiss engine support ivf method"
          )
      }
    }
    val p = new IvfParameters(
      // IVF only supported by faiss so set the engine to faiss if user doesn't
      engine = if (engine.isEmpty) Some(KnnEngine.faiss) else engine,
      spaceType = spaceType,
      nlist = nlist,
      nprobes = nprobes,
      encoder = encoder
    )
    p.validateSpaceType()
    p
  }
}

case class KnnVectorField(
    name: String,
    dimension: Int,
    parameters: KnnMethodParameters
) extends ElasticField {
  override def `type`: String = KnnVectorField.`type`
}

object KnnVectorField {
  val `type`: String = "knn_vector"

  def apply(
      name: String,
      dimension: Int,
      parameters: KnnMethodParameters
  ): KnnVectorField = {
    new KnnVectorField(
      name = name,
      dimension = dimension,
      parameters = parameters
    )
  }

}
