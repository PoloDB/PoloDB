use bson::oid::ObjectId;

pub struct ClientSession {
    _session_id: ObjectId,
}

impl ClientSession {

    pub(crate) fn new(id: ObjectId) -> ClientSession {
        ClientSession {
            _session_id: id
        }
    }

}
