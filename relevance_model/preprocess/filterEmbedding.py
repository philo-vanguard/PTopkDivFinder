import numpy as np
import argparse


# String embedFile
def readEmbedFile(embedFile, filter_embedFile=None):
    words = {}
    input = open(embedFile, 'r', encoding='utf-8')
    lines = []
    for line in input:
        if "tt__" in line or "cid__" in line:
            lines.append(line)
    input.close()
    nwords = len(lines) - 1
    splits = lines[1].split(' ')
    dim = len(splits) - 1
    embeddings = []

    if filter_embedFile is not None:
        output = open(filter_embedFile, "w", encoding="utf-8")
        output.writelines(str(nwords) + " " + str(dim) + "\n")
        output.writelines(lines)
        output.close()

    for lineId in range(len(lines)):
        splits = lines[lineId].split(' ')
        words[splits[0].strip()] = lineId
        emb = [splits[i].strip() for i in range(1, dim+1)]
        embeddings.append(emb)
    embeddings = np.array(embeddings, dtype=np.float)
    return words, embeddings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', type=str, default="../../datasets/airports/airports_embedding.emb")
    parser.add_argument('-o', '--output_file', type=str, default="../../datasets/airports/airports_embedding_filtered.emb")
    args = parser.parse_args()
    arg_dict = args.__dict__

    embedId, embeddings = readEmbedFile(arg_dict["input_file"], arg_dict["output_file"])


if __name__ == '__main__':
    main()
