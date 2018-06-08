# chasset

chasset is a toolkit for content-addressed asset storage, motivated by game development usecases. Content-addressing is
a natural paradigm for games that support user-generated content. It enables uncoordinated users to author and
efficiently distribute content without any danger of conflicts, even in the face of complex inter-dependencies.

## Mutability

Content-addressed assets must be immutable, which dovetails gracefully with production pipelines that generate
distributable data from source files for compression, format conversion, or other encoding, compilation, or
baking. Workflows that involve on-the-fly editing or reloading can be accomplished by creating and loading modified data
as new assets.

## Inter-references

References between compiled assets--for example, a 3D model's references to its textures--can be made directly
to the `Hash` of the target asset to guarantee consistent results. Taking this paradigm to the logical extreme, entire
game worlds can be identified by a single root hash specified in a configuration file.
