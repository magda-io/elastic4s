package com.sksamuel.elastic4s.requests.mappings

import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.fields.{FaissEncoder, FaissEncoderName, FaissScalarQuantizationType, HnswParameters, IvfParameters, KnnEngine, KnnMethodEncoder, KnnVectorField, SpaceType}
import com.sksamuel.elastic4s.handlers.fields.KnnVectorFieldBuilderFn
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KnnVectorFieldTest extends AnyFlatSpec with Matchers with ElasticApi {

  "A KnnVectorField" should "support empty default HnswParameters" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(name = "myfield123", dimension = 512, HnswParameters())
      )
      .string shouldBe
      """{"type":"knn_vector","dimension":512,"method":{"name":"hnsw","parameters":{}}}"""
  }

  "A KnnVectorField" should "support full nsmlib HnswParameters" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(
          name = "myfield123",
          dimension = 512,
          HnswParameters(
            engine = Some(KnnEngine.nmslib),
            spaceType = Some(SpaceType.linf),
            efConstruction = Some(100),
            m = Some(50)
          )
        )
      )
      .string shouldBe
      """{"type":"knn_vector",
        |"dimension":512,
        |"method":{"name":"hnsw","engine":"nmslib","space_type":"linf",
        |"parameters":{"ef_construction":100,"m":50}}}""".stripMargin.replace(
        "\n",
        ""
      )
  }

  "A KnnVectorField" should "throw new error when supply unsupported parameters to nsmlib HnswParameters " in {
    val exception = intercept[RuntimeException](
      KnnVectorField(
        name = "myfield123",
        dimension = 512,
        HnswParameters(
          engine = Some(KnnEngine.nmslib),
          spaceType = Some(SpaceType.innerProduct),
          efConstruction = Some(100),
          m = Some(50),
          efSearch = Some(200)
        )
      )
    )
    exception shouldBe a[RuntimeException]
    exception.getMessage shouldBe "nmslib or lucene doesn't support ef_search or encoder"
  }

  "A KnnVectorField" should "throw new error when supply unsupported parameters (space type) to faiss HnswParameters " in {
    val exception = intercept[RuntimeException](
      KnnVectorField(
        name = "myfield123",
        dimension = 512,
        HnswParameters(
          engine = Some(KnnEngine.faiss),
          spaceType = Some(SpaceType.linf),
          efConstruction = Some(100),
          m = Some(50),
          efSearch = Some(200)
        )
      )
    )
    exception shouldBe a[RuntimeException]
    exception.getMessage shouldBe "faiss engine doesn't support space_type linf"
  }

  "A KnnVectorField" should "support faiss HnswParameters" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(
          name = "myfield123",
          dimension = 512,
          HnswParameters(
            engine = Some(KnnEngine.faiss),
            spaceType = Some(SpaceType.innerProduct),
            efConstruction = Some(100),
            m = Some(50)
          )
        )
      )
      .string shouldBe
      """{"type":"knn_vector",
        |"dimension":512,
        |"method":{"name":"hnsw","engine":"faiss","space_type":"innerproduct",
        |"parameters":{"ef_construction":100,"m":50}}}""".stripMargin.replace(
        "\n",
        ""
      )
  }

  "A KnnVectorField" should "support full faiss HnswParameters (PQ)" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(
          name = "myfield123",
          dimension = 512,
          HnswParameters(
            engine = Some(KnnEngine.faiss),
            spaceType = Some(SpaceType.l2),
            efConstruction = Some(100),
            m = Some(50),
            efSearch = Some(50),
            encoder = Some(
              FaissEncoder(
                Some(FaissEncoderName.pq),
                codeSize = Some(100),
                m = Some(50)
              )
            )
          )
        )
      )
      .string shouldBe
      """{"type":"knn_vector",
        |"dimension":512,
        |"method":{"name":"hnsw","engine":"faiss","space_type":"l2",
        |"parameters":{"ef_construction":100,"m":50,"ef_search":50,
        |"encoder":{"name":"pq","parameters":{"m":50,"code_size":100}}}}}""".stripMargin.replace(
        "\n",
        ""
      )
  }

  "A KnnVectorField" should "support full faiss HnswParameters (SQ)" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(
          name = "myfield123",
          dimension = 512,
          HnswParameters(
            engine = Some(KnnEngine.faiss),
            spaceType = Some(SpaceType.innerProduct),
            efConstruction = Some(100),
            m = Some(50),
            efSearch = Some(50),
            encoder = Some(
              FaissEncoder(
                Some(FaissEncoderName.sq),
                sqType = Some(FaissScalarQuantizationType.fp16),
                sqClip = Some(true)
              )
            )
          )
        )
      )
      .string shouldBe
      """{"type":"knn_vector",
        |"dimension":512,
        |"method":{"name":"hnsw","engine":"faiss","space_type":"innerproduct",
        |"parameters":{"ef_construction":100,"m":50,"ef_search":50,
        |"encoder":{"name":"sq","parameters":{"clip":true,"type":"fp16"}}}}}""".stripMargin.replace(
        "\n",
        ""
      )
  }

  "A KnnVectorField" should "support empty IvfParameters" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(name = "myfield123", dimension = 512, IvfParameters())
      )
      .string shouldBe
      """{"type":"knn_vector","dimension":512,"method":{"name":"ivf","engine":"faiss","parameters":{}}}"""
  }

  "A KnnVectorField" should "support full faiss IvfParameters (SQ)" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(
          name = "myfield123",
          dimension = 512,
          IvfParameters(
            engine = Some(KnnEngine.faiss),
            spaceType = Some(SpaceType.innerProduct),
            nlist = Some(4),
            nprobes = Some(2),
            encoder = Some(
              FaissEncoder(
                Some(FaissEncoderName.sq),
                sqType = Some(FaissScalarQuantizationType.fp16),
                sqClip = Some(true)
              )
            )
          )
        )
      )
      .string shouldBe
      """{"type":"knn_vector",
        |"dimension":512,
        |"method":{"name":"ivf","engine":"faiss","space_type":"innerproduct",
        |"parameters":{"nlist":4,"nprobes":2,
        |"encoder":{"name":"sq","parameters":{"clip":true,"type":"fp16"}}}}}""".stripMargin.replace(
        "\n",
        ""
      )
  }

  "A KnnVectorField" should "support full lucene HnswParameters" in {
    KnnVectorFieldBuilderFn
      .build(
        KnnVectorField(
          name = "myfield123",
          dimension = 512,
          HnswParameters(
            engine = Some(KnnEngine.lucene),
            spaceType = Some(SpaceType.cosine),
            efConstruction = Some(100),
            m = Some(50)
          )
        )
      )
      .string shouldBe
      """{"type":"knn_vector",
        |"dimension":512,
        |"method":{"name":"hnsw","engine":"lucene","space_type":"cosinesimil",
        |"parameters":{"ef_construction":100,"m":50}}}""".stripMargin.replace(
        "\n",
        ""
      )
  }

}
