/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#   Mike Trinkala (trink@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"errors"
	"fmt"
	"log"
	"os"

	. "heka/pipeline"
)
// 输出插件，使用go自身的 log 输出到 stdout
var logOut = log.New(os.Stdout, "", log.LstdFlags)

// Output plugin that writes message contents out using Go standard library's
// `log` package.
type LogOutput struct {
	or OutputRunner // or => output runner 实际实现是plugins.foRunner ，没有用到
}

func (self *LogOutput) Init(config interface{}) (err error) {
	logOut.SetFlags(LogInfo.Flags())
	return err
}

func (self *LogOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	if or.Encoder() == nil {
		return errors.New("Encoder required.")
	}

	inChan := or.InChan() // inChan 是管道流包
	var (
		pack     *PipelinePack
		outBytes []byte
		e        error
	)
	for pack = range inChan {
		if outBytes, e = or.Encode(pack); e != nil {
			or.LogError(fmt.Errorf("Error encoding message: %s", e))
		} else if outBytes != nil {
			logOut.Print(string(outBytes))
		}
		or.UpdateCursor(pack.QueueCursor)
		pack.Recycle(nil)
	}
	return
}

func init() {
	RegisterPlugin("LogOutput", func() interface{} {
		return new(LogOutput)
	})
}
