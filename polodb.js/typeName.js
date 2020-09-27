/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

function typeName(ty) {
  switch (ty) {
    case 0x0A:
      return "Null";

    case 0x01:
      return "Double";

    case 0x08:
      return "Boolean";

    case 0x16:
      return "Int";

    case 0x02:
      return "String";

    case 0x07:
      return "ObjectId";

    case 0x17:
      return "Array";

    case 0x13:
      return "Document";

    case 0x05:
      return "Binary";

    default:
      return "<unknown: " + ty +">";

  }
}

module.exports = { typeName };
