// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_
#define LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_

#include <memory>
#include <string>
#include "hdfs.h"
#include "util/optional.h"

namespace log2hdfs {

class Section;

/**
 * Hdfs handle interface
 */
class HdfsHandle {
 public:
  enum Type {
    kCommand,
    kApi    /**< TODO */
  };

  /**
   * Static function to create a HdfsHandle shared_ptr
   */
  static std::shared_ptr<HdfsHandle> Init(
      std::shared_ptr<Section> section);

  /**
   * virtual desctuctor
   */
  virtual ~HdfsHandle() {}

  /**
   * Whether hdfs_path exists.
   * 
   * @param hdfs_path           hdfs path
   * 
   * @returns True if hdfs_path exists, false otherwise.
   */
  virtual bool Exists(const std::string& hdfs_path) const = 0;

  /**
   * Puth local file to hdfs
   * 
   * @param local_path          local file path
   * @param hdfs_path           hdfs path
   * 
   * @returns True if put file to hdfs success, false otherwise.
   */
  virtual bool Put(const std::string& local_path,
                   const std::string& hdfs_path) const = 0;

  /**
   * Append local file to hdfs
   * 
   * @param local_path          local file path
   * @param hdfs_path           hdfs path
   * 
   * @returns True if append file to hdfs success, false otherwise.
   */
  virtual bool Append(const std::string& local_path,
                      const std::string& hdfs_path) const = 0;

  /**
   * Delete hdfs file
   * 
   * @param hdfs_path           hdfs path
   * 
   * @returns True if delete hdfs file success, false otherwise.
   */
  virtual bool Delete(const std::string& hdfs_path) const = 0;

  /**
   * Create hdfs directory
   * 
   * @param hdfs_path           hdfs directory path
   * 
   * @returns True if create directory success, false otherwise.
   */
  virtual bool CreateDirectory(const std::string& hdfs_path) const = 0;

  /**
   * Create lzo index
   * 
   * @param hdfs_path           hdfs lzo file path
   * 
   * @returns True if create index success, false otherwise.
   */
  virtual bool LZOIndex(const std::string& hdfs_path) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_
