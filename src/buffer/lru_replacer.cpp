#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { m_size = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (m_track.size() == 0) return false;

  *frame_id = m_track.back();
  m_table.erase(*frame_id);
  m_track.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // none
  if (m_table.count(frame_id) == 0) return;

  m_track.erase((*m_table.find(frame_id)).second);
  m_table.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //full
  if (m_track.size() >= m_size) exit(1);
  //exists
  if (m_table.count(frame_id) != 0) return;

  m_track.push_front(frame_id);
  m_table.insert(make_pair(frame_id, m_track.begin()));
}

size_t LRUReplacer::Size() { return m_track.size(); }